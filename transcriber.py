import asyncio
import hashlib
import json
import os
from pydantic import BaseModel, Field
import requests
import time
from typing import Dict, Optional, Any, Tuple, List
import re

WHISPER_API_URL = "http://transcriber:8005/transcriber"
WHISPER_TIMEOUT = 180
MAX_ATTEMPTS = 2


def _debug_log(message: str) -> None:
    """Debug logging function"""
    print(f"[WhisperX Pipe Debug] {message}")


class Pipe:
    class Valves(BaseModel):
        LANGUAGE: str = Field(
            default="auto",
            description="Language code or 'auto' for auto-detect.",
        )
        TIMEOUT: float = Field(
            default=180.0,
            description="Maximum seconds to wait for a job to finish.",
        )
        UPLOADS_BASE_PATH: str = Field(
            default="/app/backend/data/uploads",
            description="Filesystem path where Open WebUI stores uploaded files.",
        )

    class UserValves(BaseModel):
        """
        TODO:
        make it work to chose language manually if need it
        """

        LANGUAGE: str = Field(
            default="auto",
            description="Выберите язык",
            json_schema_extra={"enum": ["auto", "ru", "en", "fr", "de", "es"]},
        )

    def __init__(self):
        self.type = "manifold"
        self.id = "whisperx_pipe"
        self.name = "WhisperX Transcription"
        self.valves = self.Valves()
        self.user_valves = self.UserValves()

        # Diarization cache: {file_hash: (diarization, num_participants)}
        self.file_diarization_cache = {}
        # Chat state: {chat_id: {...}}
        self.chat_processing_states = {}
        # Chat file history (for differ old and new files): {chat_id: {file_id: {"message_id": str, "transcription": str}}}
        self.chat_file_history = {}
        # Files Cache: {file_hash: {"processing": bool, "result": str, "file_id": str}}
        self.file_cache = {}

        self.chats_lock = asyncio.Lock()

    def _get_file_hash(
        self,
        chat_id: str,
        message_id: str,
        file_id: str,
        diarization: bool = False,
        num_participants: int = 1,
    ) -> str:
        """Unique file hash"""
        combined = f"{chat_id}_{message_id}_{file_id}_diarization_{diarization}_participants_{num_participants}"
        return hashlib.md5(combined.encode()).hexdigest()

    async def _get_chat_processing_state(self, chat_id: str) -> dict:
        """Unique state for chat"""
        async with self.chats_lock:
            if chat_id not in self.chat_processing_states:
                self.chat_processing_states[chat_id] = {
                    "processing_files": set(),
                    "processing_lock": asyncio.Lock(),
                }
                _debug_log(f"Создано новое состояние обработки для чата {chat_id}")
            return self.chat_processing_states[chat_id]

    def _get_chat_file_history(self, chat_id: str) -> dict:
        """Get or create chat history for tracking old and new files"""
        if chat_id not in self.chat_file_history:
            self.chat_file_history[chat_id] = {}
        return self.chat_file_history[chat_id]

    def _is_file_processed_in_other_message(
        self, chat_id: str, file_id: str, current_message_id: str
    ) -> bool:
        """Checking old or new file (it is from past message(s) or current"""
        if chat_id not in self.chat_file_history:
            return False

        file_history = self.chat_file_history[chat_id]
        if file_id not in file_history:
            return False

        return file_history[file_id]["message_id"] != current_message_id

    async def _get_file_diarization_settings(
        self,
        chat_id: str,
        message_id: str,
        file_id: str,
        filename: str,
        __event_call__=None,
    ) -> Tuple[bool, int]:
        """
        Diarization settings for particalar file
        """
        # Hash for search in cache
        temp_hash = self._get_file_hash(chat_id, message_id, file_id, False, 1)

        if temp_hash in self.file_diarization_cache:
            settings = self.file_diarization_cache[temp_hash]
            _debug_log(
                f"Используем кэшированные настройки для файла {filename}: "
                f"diarization={settings[0]}, num_participants={settings[1]}"
            )
            return settings

        if not __event_call__:
            _debug_log(
                f"Нет __event_call__, используем настройки по умолчанию для файла {filename}"
            )
            return False, 1

        _debug_log(f"Запрашиваем настройки диаризации для файла {filename}")
        diarization = False
        num_participants = 1

        try:
            diarize_prompt = {
                "type": "confirmation",
                "data": {
                    "title": f"Распознавание спикеров для {filename}",
                    "message": "Есть ли в этой записи несколько спикеров?",
                    "description": "Эта настройка будет применена только к этому файлу",
                },
            }

            diarize_response = await __event_call__(diarize_prompt)
            diarization = bool(diarize_response)

            if diarization:
                participants_prompt = {
                    "type": "input",
                    "data": {
                        "title": f"Количество участников для {filename}",
                        "message": "Введите количество участников (1-100):",
                        "placeholder": "2",
                        "description": "Это значение будет использоваться только для этого файла",
                    },
                }

                participants_response = await __event_call__(participants_prompt)
                try:
                    num_participants = (
                        int(participants_response) if participants_response else 1
                    )
                    num_participants = max(1, min(100, num_participants))
                except ValueError:
                    num_participants = 1

            actual_hash = self._get_file_hash(
                chat_id, message_id, file_id, diarization, num_participants
            )
            self.file_diarization_cache[actual_hash] = (diarization, num_participants)

            _debug_log(
                f"Сохранены настройки для файла {filename}: "
                f"diarization={diarization}, num_participants={num_participants}"
            )

            return diarization, num_participants

        except Exception as e:
            _debug_log(f"Ошибка при запросе настроек для файла {filename}: {e}")
            default_hash = self._get_file_hash(chat_id, message_id, file_id, False, 1)
            self.file_diarization_cache[default_hash] = (False, 1)
            return False, 1

    async def _transcribe_file(
        self,
        file_path: str,
        fname: str,
        file_id: str,
        chat_id: str,
        message_id: str,
        language: str,
        diarization: bool,
        num_participants: int,
        chat_state: dict,
    ) -> dict:
        """
        Transcribe one file (check for doubles)
        """
        processing_files = chat_state["processing_files"]
        processing_lock = chat_state["processing_lock"]

        async with processing_lock:
            if file_id in processing_files:
                _debug_log(
                    f"Файл {fname} (id: {file_id}) уже обрабатывается в чате {chat_id}, пропускаем"
                )
                return {"success": False, "already_processing": True}

            processing_files.add(file_id)

        _debug_log(
            f"Начинаем обработку файла {fname} (id: {file_id}) в чате {chat_id}, сообщение {message_id}"
        )

        try:
            try:
                with open(file_path, "rb") as fh:
                    file_bytes = fh.read()
            except Exception as e:
                return {"success": False, "error": f"Не удалось прочитать файл: {e}"}

            try:
                files = {"file": (fname, file_bytes, "application/octet-stream")}
                params = {
                    "language": language,
                    "num_participants": num_participants,
                    "diarization": str(diarization).lower(),
                }

                _debug_log(f"Отправляем запрос для {fname} с параметрами: {params}")

                result = None
                for attempt in range(MAX_ATTEMPTS):
                    try:
                        response = requests.post(
                            WHISPER_API_URL,
                            params=params,
                            files=files,
                            timeout=WHISPER_TIMEOUT,
                        )

                        if response.status_code == 200:
                            result = response.json()
                            if (
                                not result
                                or "segments" not in result
                                or not result["segments"]
                            ):
                                _debug_log(
                                    f"API вернул пустой результат для {fname}: {result}"
                                )
                            break
                        elif attempt < MAX_ATTEMPTS - 1:
                            _debug_log(
                                f"Попытка {attempt + 1} не удалась, повторяем..."
                            )
                            time.sleep(1)
                            continue
                        else:
                            error_msg = f"API request failed: {response.status_code} {response.text[:200]}"
                            raise Exception(error_msg)

                    except requests.exceptions.Timeout:
                        if attempt < MAX_ATTEMPTS - 1:
                            _debug_log(f"Таймаут, попытка {attempt + 1}, повторяем...")
                            time.sleep(2)
                        else:
                            raise Exception(f"Таймаут запроса для {fname}")

                    except Exception as e:
                        if attempt < MAX_ATTEMPTS - 1:
                            _debug_log(f"Ошибка на попытке {attempt + 1}: {e}")
                            time.sleep(1)
                            continue
                        else:
                            raise e

                return {"success": True, "result": result}

            except requests.exceptions.ConnectionError as e:
                return {
                    "success": False,
                    "error": f"Ошибка подключения к сервису транскрибации: {e}",
                }
            except Exception as e:
                return {"success": False, "error": f"{type(e).__name__}: {str(e)}"}

        finally:
            async with processing_lock:
                processing_files.discard(file_id)
            _debug_log(
                f"Завершена обработка файла {fname} (id: {file_id}) в чате {chat_id}"
            )

    def _convert_segments_to_text(self, segments: list, diarization: bool) -> str:
        """Конвертирует сегменты WhisperX в читаемый текст."""
        try:
            if not segments:
                return "Транскрипция недоступна."

            speaker_lines = {}
            for seg in segments:
                if isinstance(seg, dict):
                    speaker = seg.get("speaker", "Unknown")
                    text = seg.get("text", "").strip()
                    if not text:
                        text = (
                            seg.get("word", "").strip()
                            or seg.get("segment", "").strip()
                        )
                else:
                    speaker = "Unknown"
                    text = str(seg).strip()

                if speaker not in speaker_lines:
                    speaker_lines[speaker] = []
                if text:
                    speaker_lines[speaker].append(text)

            lines = []
            for speaker, texts in speaker_lines.items():
                if texts:
                    combined_text = " ".join(texts)
                    combined_text = re.sub(r"\s+", " ", combined_text).strip()
                    if diarization:
                        lines.append(f"**{speaker}**: {combined_text}")
                    else:
                        lines.append(f"{combined_text}")

            return "\n\n".join(lines) if lines else str(segments)

        except Exception as e:
            _debug_log(f"Error converting format: {e}")
            try:
                return json.dumps(segments, ensure_ascii=False, indent=2)
            except:
                return str(segments)

    async def _emit(self, emitter, message: str, done: bool = False):
        """Event emitter function"""
        if emitter:
            try:
                await emitter(
                    {
                        "type": "status",
                        "data": {
                            "description": message,
                            "done": done,
                        },
                    }
                )
            except Exception as e:
                _debug_log(f"Error emitting status: {e}")

    async def pipe(
        self,
        body: dict,
        __user__: Optional[dict] = None,
        __event_emitter__=None,
        __event_call__=None,
        __metadata__: dict = None,
    ):
        """Asynchronous transcription against WhisperX API."""

        if not __metadata__ or "files" not in __metadata__:
            return "Ошибка: Файлы не предоставлены."

        chat_id = __metadata__["chat_id"]
        message_id = __metadata__["message_id"]
        _debug_log(f"Обработка запроса для чата: {chat_id}, сообщение: {message_id}")

        files = __metadata__.get("files", [])
        if not files:
            return "Ошибка: Файлы не предоставлены."

        new_files = []
        file_ids = []
        for main_info in files:
            try:
                file_info = main_info.get("file", None)
                if file_info:
                    new_files.append(file_info)
                    file_id = file_info.get("id")
                    if file_id:
                        file_ids.append(file_id)
            except Exception as e:
                _debug_log(f"Ошибка при обработке файла: {e}")
                continue

        if not new_files:
            return "Ошибка: Не удалось извлечь информацию о файлов."

        result = None
        language = body.get("language", self.valves.LANGUAGE)

        _debug_log(
            f"Обрабатываем {len(new_files)} файлов в чате {chat_id}, сообщение {message_id}"
        )

        try:
            chat_state = await self._get_chat_processing_state(chat_id)

            chat_file_history = self._get_chat_file_history(chat_id)

            self.file_diarization_cache.clear()

            transcriptions = []
            unsupported_filenames = []

            for file in new_files:
                file_id = file.get("id")
                fname = file.get("filename")

                if file_id in chat_file_history:
                    file_history = chat_file_history[file_id]
                    if file_history["message_id"] == message_id:
                        _debug_log(
                            f"Файл {fname} (id: {file_id}) уже обработан в сообщении {message_id}, возвращаем кэш"
                        )
                        transcriptions.append(
                            f"\nТранскрипция для **{fname}**\n\n{file_history['transcription']}"
                        )
                        continue
                    else:
                        _debug_log(
                            f"Файл {fname} (id: {file_id}) уже обработан в другом сообщении, пропускаем"
                        )
                        continue

                file_path = None
                if file.get("path"):
                    file_path = file.get("path")
                elif self.valves.UPLOADS_BASE_PATH and file_id and fname:
                    potential_path = (
                        f"{self.valves.UPLOADS_BASE_PATH}/{file_id}_{fname}"
                    )
                    if os.path.exists(potential_path):
                        file_path = potential_path
                    else:
                        alternative_path = f"{self.valves.UPLOADS_BASE_PATH}/{fname}"
                        if os.path.exists(alternative_path):
                            file_path = alternative_path

                if not fname or not file_id or not file_path:
                    unsupported_filenames.append(fname if fname else "unknown")
                    continue

                if not os.path.exists(file_path):
                    _debug_log(f"Файл не найден: {file_path}")
                    unsupported_filenames.append(fname)
                    continue

                diarization, num_participants = (
                    await self._get_file_diarization_settings(
                        chat_id, message_id, file_id, fname, __event_call__
                    )
                )

                file_hash = self._get_file_hash(
                    chat_id, message_id, file_id, diarization, num_participants
                )

                async with self.chats_lock:
                    if file_hash in self.file_cache:
                        cached_result = self.file_cache[file_hash]
                        if cached_result["processing"]:
                            _debug_log(
                                f"Файл {fname} уже обрабатывается, ожидаем завершения"
                            )
                            for _ in range(50):
                                await asyncio.sleep(0.1)
                                if not self.file_cache[file_hash]["processing"]:
                                    transcriptions.append(
                                        f"\nТранскрипция для **{fname}**\n\n{cached_result['result']}"
                                    )
                                    break
                            else:
                                transcriptions.append(
                                    f"\n**{fname}**\n\nОшибка: Таймаут ожидания обработки файла"
                                )
                            continue
                        else:
                            _debug_log(
                                f"Файл {fname} уже обработан, возвращаем из кэша"
                            )
                            transcriptions.append(
                                f"\nТранскрипция для **{fname}**\n\n{cached_result['result']}"
                            )
                            continue

                    self.file_cache[file_hash] = {
                        "processing": True,
                        "result": None,
                        "file_id": file_id,
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "filename": fname,
                        "diarization": diarization,
                        "num_participants": num_participants,
                    }

                await self._emit(__event_emitter__, f"Начинаем транскрибацию {fname}")

                result_data = await self._transcribe_file(
                    file_path,
                    fname,
                    file_id,
                    chat_id,
                    message_id,
                    language,
                    diarization,
                    num_participants,
                    chat_state,
                )

                if not result_data["success"] and result_data.get("already_processing"):
                    _debug_log(f"Пропускаем {fname} - уже обрабатывается в этом чате")
                    continue

                if result_data["success"]:
                    api_result = result_data["result"]
                    if (
                        api_result
                        and isinstance(api_result, dict)
                        and "segments" in api_result
                    ):
                        transcription_text = self._convert_segments_to_text(
                            api_result["segments"], diarization
                        )
                    else:
                        transcription_text = (
                            str(api_result) if api_result else "Пустой результат"
                        )

                    chat_file_history[file_id] = {
                        "message_id": message_id,
                        "transcription": transcription_text,
                    }

                    transcriptions.append(
                        f"\nТранскрипция для **{fname}**\n\n{transcription_text}"
                    )
                    await self._emit(
                        __event_emitter__, f"Завершена транскрибация {fname}"
                    )

                    async with self.chats_lock:
                        if file_hash in self.file_cache:
                            self.file_cache[file_hash].update(
                                {
                                    "processing": False,
                                    "result": transcription_text,
                                }
                            )
                else:
                    error_msg = result_data["error"]
                    _debug_log(f"Ошибка при обработке {fname}: {error_msg}")
                    await self._emit(__event_emitter__, f"Ошибка: {error_msg}")
                    transcriptions.append(f"\n**{fname}**\n\nОшибка: {error_msg}")

                    async with self.chats_lock:
                        if file_hash in self.file_cache:
                            self.file_cache[file_hash].update(
                                {
                                    "processing": False,
                                    "result": f"Ошибка: {error_msg}",
                                }
                            )

            if not transcriptions:
                if unsupported_filenames:
                    result = (
                        f"Ошибка: Не найдено поддерживаемых аудиофайлов. "
                        f"Неподдерживаемые файлы: {', '.join(unsupported_filenames)}."
                    )
                else:
                    result = "Все файлы уже были обработаны ранее в этом чате."
            else:
                result = "\n\n".join(transcriptions)

            await self._emit(__event_emitter__, "Все файлы обработаны.", done=True)

            _debug_log(
                f"Завершено. Обработано файлов в чате {chat_id}, сообщение {message_id}: {len(transcriptions)}"
            )

            return result

        except Exception as e:
            _debug_log(f"Критическая ошибка в pipe: {e}")
            result = f"Критическая ошибка при обработке: {str(e)}"
            return result
