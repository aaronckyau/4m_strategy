"""
extensions.py - 共用物件（跨 Blueprint 共用）
============================================================================
gemini_client 和 prompt_manager 在此初始化一次，
各 Blueprint 透過 import 取用，避免重複建立。
============================================================================
"""
from google import genai
from prompt_manager import PromptManager
from config import Config
from logger import get_logger

_log = get_logger(__name__)

try:
    gemini_client = genai.Client(api_key=Config.GEMINI_API_KEY)
except Exception as e:
    _log.error("Gemini client 初始化失敗: %s", e)
    raise

try:
    prompt_manager = PromptManager(Config.PROMPTS_PATH)
except Exception as e:
    _log.error("PromptManager 初始化失敗 (%s): %s", Config.PROMPTS_PATH, e)
    raise
