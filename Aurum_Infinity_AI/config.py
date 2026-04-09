"""
config.py - 應用程式設定
============================================================================
從 app.py 抽出的 Config class，集中管理所有配置項。
支援 Dev / Production 環境區分，由 FLASK_ENV 環境變數切換。
============================================================================
"""
import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    """基礎配置（所有環境共用）"""
    GEMINI_API_KEY  = os.getenv("GEMINI_API_KEY")
    FMP_API_KEY     = os.getenv("FMP_API_KEY")
    GEMINI_MODEL    = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
    DEFAULT_TICKER  = 'NVDA'
    PROMPTS_PATH    = os.path.join(os.path.dirname(__file__), 'prompts', 'prompts.yaml')
    API_MAX_TOKENS  = 8000
    API_MAX_RETRIES = 2
    API_RETRY_DELAY = 5

    @classmethod
    def validate(cls):
        """啟動時驗證必要環境變數，缺少則立即報錯"""
        missing = []
        if not cls.GEMINI_API_KEY:
            missing.append("GEMINI_API_KEY")
        if not os.getenv("FLASK_SECRET_KEY"):
            missing.append("FLASK_SECRET_KEY")
        if missing:
            raise RuntimeError(
                f"缺少必要環境變數: {', '.join(missing)}，請檢查 .env 檔案"
            )


class DevConfig(Config):
    """開發環境配置"""
    DEBUG = True
    API_MAX_RETRIES = 1
    API_RETRY_DELAY = 2

    @classmethod
    def validate(cls):
        """開發環境僅需 GEMINI_API_KEY，SECRET_KEY 可用預設值"""
        if not cls.GEMINI_API_KEY:
            raise RuntimeError("缺少必要環境變數: GEMINI_API_KEY，請檢查 .env 檔案")


class ProdConfig(Config):
    """生產環境配置"""
    DEBUG = False


# 依 FLASK_ENV 自動選擇
_env_map = {
    'development': DevConfig,
    'production': ProdConfig,
}

def get_config():
    """根據 FLASK_ENV 回傳對應的 Config class"""
    env = os.getenv('FLASK_ENV', 'production').lower()
    return _env_map.get(env, ProdConfig)
