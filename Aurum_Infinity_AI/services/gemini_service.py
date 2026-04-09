"""
gemini_service.py - Gemini API 呼叫 + 文字處理工具
============================================================================
從 app.py 抽出的共用服務，供各 Blueprint 使用。
============================================================================
"""
import re
import time

from google.genai import types
from google.genai.errors import APIError, ClientError, ServerError

from config import Config
from extensions import gemini_client
from logger import get_logger

_log = get_logger(__name__)


# ============================================================================
# Gemini API 呼叫
# ============================================================================

def call_gemini_api(prompt: str, use_search: bool = True) -> str:
    """調用 Gemini API，支援聯網搜索"""
    config_params: dict = {
        "temperature": 0.7,
        "max_output_tokens": Config.API_MAX_TOKENS,
    }
    if use_search:
        config_params["tools"] = [types.Tool(google_search=types.GoogleSearch())]

    config = types.GenerateContentConfig(**config_params)

    for attempt in range(Config.API_MAX_RETRIES + 1):
        try:
            response = gemini_client.models.generate_content(
                model=Config.GEMINI_MODEL,
                contents=prompt,
                config=config,
            )
            return response.text if response.text else "⚠️ API 回覆為空或被安全過濾。"
        except ClientError as e:
            # 4xx 錯誤（API Key 無效、配額超限、內容被封鎖等），不需重試
            _log.error("Gemini API 用戶端錯誤 (不重試): %s", e)
            return "⚠️ API 請求被拒絕，請檢查 API Key 或請求內容。"
        except ServerError as e:
            # 5xx 伺服器端錯誤，可重試
            _log.warning("Gemini API 伺服器錯誤 (attempt %d): %s", attempt + 1, e)
            if attempt < Config.API_MAX_RETRIES:
                time.sleep(Config.API_RETRY_DELAY * (2 ** attempt))
        except (ConnectionError, TimeoutError, OSError) as e:
            _log.warning("Gemini API 網路錯誤 (attempt %d): %s", attempt + 1, e)
            if attempt < Config.API_MAX_RETRIES:
                time.sleep(Config.API_RETRY_DELAY * (2 ** attempt))
        except ValueError as e:
            # API 回應解析錯誤，不需重試
            _log.error("Gemini API 回應解析失敗: %s", e)
            return "⚠️ API 回應格式異常。"
        except APIError as e:
            # 其他 Google API 錯誤
            _log.error("Gemini API 錯誤 (attempt %d): %s", attempt + 1, e)
            if attempt < Config.API_MAX_RETRIES:
                time.sleep(Config.API_RETRY_DELAY * (2 ** attempt))
        except Exception as e:
            _log.error("Gemini API 未預期錯誤 (attempt %d): %s: %s",
                       attempt + 1, type(e).__name__, e)
            if attempt < Config.API_MAX_RETRIES:
                time.sleep(Config.API_RETRY_DELAY * (2 ** attempt))
    return "⚠️ API 請求失敗。"


# ============================================================================
# 文字處理工具
# ============================================================================

def extract_card_summary(text: str) -> str | None:
    """從 AI 回傳文字中提取 <card-summary> 摘要內容"""
    m = re.search(r'<card-summary>([\s\S]*?)</card-summary>', text, re.IGNORECASE)
    return m.group(1).strip() if m else None


def strip_card_summary(html: str) -> str:
    """移除 <card-summary>...</card-summary> 標籤，避免摘要出現在報告內容中"""
    return re.sub(r'<card-summary>[\s\S]*?</card-summary>\s*', '', html, flags=re.IGNORECASE)
