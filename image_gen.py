import os
import re
import base64
import requests
import time
from typing import Optional


class EventEmitter:
    """EventEmitter для Open WebUI"""

    def __init__(self, event_emitter):
        self.emit = event_emitter


async def generate_image_auto(
    body: dict,
    __user__: Optional[dict] = None,
    __event_emitter__=None,
) -> str:
    """Generate image if keywords are detected in user message"""
    emitter = EventEmitter(__event_emitter__)

    IMAGE_TRIGGERS = [
        r"\b(создай|сгенерируй|нарисуй|сделай|покажи)\s+(картинк|изображен|рисунок|фото)",
        r"\bнарисуй\b",
    ]

    user_message = body.get("messages", [{}])[-1].get("content", "")

    should_generate = any(
        re.search(pattern, user_message.lower()) for pattern in IMAGE_TRIGGERS
    )

    if not should_generate:
        return None

    await emitter.emit(
        {
            "type": "status",
            "data": {
                "description": "☝️ Запрос на генерацию изображения...",
                "done": False,
            },
        }
    )

    FLUX_API_URL = YOUR_IMAGE_SERVER # should change on your real value
    FLUX_TIMEOUT = 30
    OLLAMA_URL = YOUR_OLLAMA_SERVER # should change on your real value
    OLLAMA_TIMEOUT = 10
    MAX_ATTEMPTS = 2

    prompt = None

    try:
        for attempt in range(MAX_ATTEMPTS):
            prompt_data = {
                "model": MODEL_HERE, # should change on your real value
                "prompt": f"""Rephrase the following text into a short image generation prompt for FLUX model (80 token max). 
                Include visual details, style, lighting, and composition: {user_message}
                
                Return ONLY the prompt, nothing else.""",
                "stream": False,
            }

            try:
                prompt_response = requests.post(
                    OLLAMA_URL, json=prompt_data, timeout=OLLAMA_TIMEOUT
                )

                if prompt_response.status_code == 200:
                    prompt = prompt_response.json()["response"].strip()[:100]
                    break

                if attempt == MAX_ATTEMPTS - 1:
                    await emitter.emit(
                        {
                            "type": "status",
                            "data": {
                                "description": "❌ Ошибка создания промпта",
                                "done": True,
                            },
                        }
                    )
                    return "❌ Не удалось создать промпт для генерации изображения."
                else:
                    time.sleep(1)
                    continue

            except requests.exceptions.RequestException as e:
                if attempt == MAX_ATTEMPTS - 1:
                    await emitter.emit(
                        {
                            "type": "status",
                            "data": {
                                "description": "❌ Ошибка соединения с Ollama",
                                "done": True,
                            },
                        }
                    )
                    return f"❌ Ошибка соединения с Ollama: {str(e)}"
                else:
                    time.sleep(1)
                    continue

        if not prompt:
            await emitter.emit(
                {
                    "type": "status",
                    "data": {
                        "description": "❌ Не удалось создать промпт",
                        "done": True,
                    },
                }
            )
            return "❌ Не удалось создать промпт для генерации."

        await emitter.emit(
            {
                "type": "status",
                "data": {
                    "description": f"🎨 Генерация изображения",
                    "done": False,
                },
            }
        )

        payload = {
            "prompt": prompt,
            "height": 512,
            "width": 512,
            "num_inference_steps": 3,
            "guidance_scale": 0.0,
        }

        for attempt in range(MAX_ATTEMPTS):
            try:
                image_response = requests.post(
                    FLUX_API_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=FLUX_TIMEOUT,
                )

                if image_response.status_code == 200:
                    image_bytes = image_response.content
                    b64_image = base64.b64encode(image_bytes).decode("utf-8")
                    image_url = f"data:image/png;base64,{b64_image}"

                    await emitter.emit(
                        {
                            "type": "status",
                            "data": {
                                "description": "✅ Изображение готово!",
                                "done": True,
                            },
                        }
                    )

                    await emitter.emit(
                        {
                            "type": "message",
                            "data": {"content": f"![Generated Image]({image_url})"},
                        }
                    )

                    return ""

                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(1)
                    continue
                else:
                    error_detail = image_response.json().get("detail", "Unknown error")
                    await emitter.emit(
                        {
                            "type": "status",
                            "data": {
                                "description": "❌ Ошибка генерации изображения",
                                "done": True,
                            },
                        }
                    )
                    return f"❌ Не удалось сгенерировать изображение: {error_detail}"

            except requests.exceptions.Timeout:
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(2)
                else:
                    await emitter.emit(
                        {
                            "type": "status",
                            "data": {
                                "description": f"⏱️ Превышен таймаут",
                                "done": True,
                            },
                        }
                    )
                    return f"❌ Таймаут после {FLUX_TIMEOUT} секунд."

            except requests.exceptions.ConnectionError:
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(1)
                else:
                    await emitter.emit(
                        {
                            "type": "status",
                            "data": {
                                "description": "❌ Ошибка подключения",
                                "done": True,
                            },
                        }
                    )
                    return "❌ Не удалось подключиться к FLUX."

            except Exception as e:
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(1)
                    continue
                else:
                    await emitter.emit(
                        {
                            "type": "status",
                            "data": {
                                "description": f"❌ Ошибка при генерации",
                                "done": True,
                            },
                        }
                    )
                    return f"❌ Ошибка: {type(e).__name__}: {str(e)}"

    except Exception as e:
        await emitter.emit(
            {
                "type": "status",
                "data": {
                    "description": f"❌ Непредвиденная ошибка",
                    "done": True,
                },
            }
        )
        return f"❌ Непредвиденная ошибка: {type(e).__name__}: {str(e)}"


class Filter:
    """Calss for auto image generation in chat"""

    def __init__(self):
        self.name = "Auto Image Generator"
        self.description = "Automatically generates images when user requests them"

    async def inlet(
        self, body: dict, __user__: Optional[dict] = None, __event_emitter__=None
    ) -> dict:
        """
        Inlet перехватывает входящие сообщения ДО отправки в LLM
        """
        result = await generate_image_auto(body, __user__, __event_emitter__)

        if result:
            body["messages"].append({"role": "assistant", "content": result})

        return body

    async def outlet(
        self, body: dict, __user__: Optional[dict] = None, __event_emitter__=None
    ) -> dict:
        """
        Outlet for LLM response
        """
        return body
