"""
Prompt Manager - 從 YAML 檔案載入並渲染 Prompt 模板
============================================================================
用法：
    from prompt_manager import PromptManager
    
    pm = PromptManager("prompts/prompts.yaml")
    prompt = pm.build("biz", ticker="NVDA", stock_name="NVIDIA", exchange="NASDAQ", today="2025/01/15")
============================================================================
"""

import os
import yaml
from typing import Dict, Optional
from logger import get_logger

_log = get_logger(__name__)


class PromptManager:
    """管理所有 prompt 模板的載入、變數替換與組裝"""

    def __init__(self, yaml_path: str):
        """
        初始化 PromptManager
        
        Args:
            yaml_path: YAML prompt 設定檔路徑
        """
        self.yaml_path = yaml_path
        self._config = self._load_yaml()
        self._last_modified = os.path.getmtime(yaml_path)

    def _load_yaml(self) -> dict:
        """載入 YAML 設定檔"""
        with open(self.yaml_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _reload_if_changed(self):
        """如果 YAML 檔案有更新，自動重新載入（開發模式很方便）"""
        current_mtime = os.path.getmtime(self.yaml_path)
        if current_mtime != self._last_modified:
            _log.info("偵測到 YAML 更新，重新載入...")
            self._config = self._load_yaml()
            self._last_modified = current_mtime

    @staticmethod
    def _normalize_exchange(exchange: str) -> str:
        """
        把原始交易所代碼統一對應到 3 個區域代碼：
          NYSE / NASDAQ / AMEX  → US
          HKEX / HK             → HK
          SHH / SHZ             → CN
          其他                  → 原值（交給 _default 處理）
        """
        ex = exchange.upper().strip()
        if ex in ('NYSE', 'NASDAQ', 'AMEX'):
            return 'US'
        if ex in ('HKEX', 'HK'):
            return 'HK'
        if ex in ('SHH', 'SHZ'):
            return 'CN'
        return ex

    def _get_exchange_context(self, exchange: str) -> dict:
        """根據交易所取得對應的設定（資料來源、幣值、法律重點等）"""
        normalized   = self._normalize_exchange(exchange)
        exchange_map = self._config.get('exchange_context', {})
        context      = exchange_map.get(normalized, exchange_map.get('_default', {}))
        return {
            'data_source':    context.get('data_source', ''),
            'currency':       context.get('currency', ''),
            'legal_focus':    context.get('legal_focus', ''),
            'extra_analysis': context.get('extra_analysis', ''),
        }

    def get_section_names(self) -> Dict[str, str]:
        """取得所有可用的 section 名稱（用於前端顯示）"""
        sections = self._config.get('sections', {})
        return {key: val.get('name', key) for key, val in sections.items()}

    def build(
        self,
        section: str,
        ticker: str,
        stock_name: str,
        exchange: str,
        today: str,
        chinese_name: str = "",
        **extra_vars
    ) -> str:
        """
        組裝完整的 prompt
        
        組裝順序：
        1. global.system_role（角色設定）
        2. section.prompt（該分析項的主體模板）
        3. global.format_rules（格式要求）
        
        所有 {variable} 都會被替換為實際值。
        
        Args:
            section: prompt 分類 key（如 "biz", "finance" 等）
            ticker: 股票代碼
            stock_name: 公司英文名稱
            exchange: 交易所代碼
            today: 今天日期
            chinese_name: 公司中文名稱（可選）
            **extra_vars: 額外的替換變數
            
        Returns:
            組裝完成的 prompt 字串
        """
        self._reload_if_changed()

        # 取得各部分模板
        global_cfg = self._config.get('global', {})
        sections = self._config.get('sections', {})
        section_cfg = sections.get(section)

        if not section_cfg:
            available = ', '.join(sections.keys())
            return f"未知的分析類別: {section}。可用類別: {available}"

        system_role = global_cfg.get('system_role', '')
        section_prompt = section_cfg.get('prompt', '')
        format_rules = global_cfg.get('format_rules', '')

        # 準備替換變數
        exchange_ctx = self._get_exchange_context(exchange)
        variables = {
            'ticker': ticker,
            'stock_name': stock_name,
            'exchange': exchange,
            'today': today,
            'chinese_name': chinese_name,
            **exchange_ctx,
            **extra_vars,
        }

        # 組裝完整 prompt
        full_prompt = f"{system_role}\n\n{section_prompt}\n\n{format_rules}"

        # DB 驅動變數：只在 prompt 中有 {key} 時才查詢（避免無謂 DB 讀取）
        from services.data_vars import DATA_VAR_REGISTRY, resolve_data_vars
        needed = [k for k in DATA_VAR_REGISTRY if f'{{{k}}}' in full_prompt]
        if needed:
            data_vars = resolve_data_vars(ticker)
            for k in needed:
                variables[k] = data_vars.get(k, '')

        # 替換所有變數
        for key, value in variables.items():
            full_prompt = full_prompt.replace(f'{{{key}}}', str(value))

        return full_prompt.strip()


    def get_section_prompt(self, section: str) -> str:
        """取得某個 section 的原始 prompt 文字（供 Admin 編輯頁顯示）"""
        self._reload_if_changed()
        sections = self._config.get('sections', {})
        return sections.get(section, {}).get('prompt', '')

    def update_section_prompt(self, section: str, new_prompt: str):
        """更新某個 section 的 prompt 並寫回 YAML 檔案（供 Admin 編輯頁儲存）"""
        self._reload_if_changed()
        if 'sections' not in self._config:
            raise ValueError("YAML 格式錯誤：找不到 sections")
        if section not in self._config['sections']:
            raise ValueError(f"找不到 section：{section}")

        self._config['sections'][section]['prompt'] = new_prompt

        with open(self.yaml_path, 'w', encoding='utf-8') as f:
            yaml.dump(self._config, f,
                      allow_unicode=True,
                      default_flow_style=False,
                      sort_keys=False)

        self._last_modified = os.path.getmtime(self.yaml_path)
        _log.info("已儲存 %s → %s", section, self.yaml_path)

    @staticmethod
    def build_translation_prompt(source_html: str, target_lang: str) -> str:
        """
        建立翻譯 prompt，將已生成的繁中 HTML 報告翻譯成目標語言。

        Args:
            source_html: 繁體中文的 HTML 報告內容
            target_lang: 目標語言代碼（zh_cn 或 en）

        Returns:
            給 Gemini 的翻譯 prompt 字串
        """
        lang_name = {
            "zh_cn": "簡體中文（Simplified Chinese）",
            "en":    "English",
        }.get(target_lang, "English")

        return (
            f"Please translate the following financial analysis report into {lang_name}.\n\n"
            "Requirements:\n"
            "- Preserve ALL HTML tags and formatting exactly (headings, tables, bold, lists)\n"
            "- Preserve the <card-summary>...</card-summary> tag and translate its content\n"
            "- Use standard financial terminology appropriate for the target language\n"
            "- Do NOT translate: numbers, stock tickers, company English names, URLs\n"
            "- Output ONLY the translated HTML content, no explanations or wrappers\n\n"
            "---\n"
            f"{source_html}"
        )

    def list_variables(self, section: str) -> list:
        """列出某個 section 中使用的所有變數（方便除錯）"""
        import re
        sections = self._config.get('sections', {})
        section_cfg = sections.get(section, {})
        prompt = section_cfg.get('prompt', '')
        return list(set(re.findall(r'\{(\w+)\}', prompt)))


# ============================================================================
# 快速測試
# ============================================================================
if __name__ == "__main__":
    pm = PromptManager("prompts/prompts.yaml")

    # 列出所有可用 section
    print("=== 可用分析類別 ===")
    for key, name in pm.get_section_names().items():
        print(f"  {key}: {name}")

    # 測試組裝 biz prompt
    print("\n=== 測試 biz prompt（美股）===")
    prompt = pm.build(
        section="biz",
        ticker="NVDA",
        stock_name="NVIDIA Corporation",
        exchange="NASDAQ",
        today="2025/01/15",
        chinese_name="輝達"
    )
    print(prompt[:500] + "\n...(truncated)")

    # 測試組裝 biz prompt（港股）
    print("\n=== 測試 biz prompt（港股）===")
    prompt_hk = pm.build(
        section="biz",
        ticker="0700.HK",
        stock_name="Tencent Holdings",
        exchange="HKEX",
        today="2025/01/15",
        chinese_name="騰訊控股"
    )
    print(prompt_hk[:500] + "\n...(truncated)")

    # 列出 biz 的變數
    print(f"\n=== biz 使用的變數 ===")
    print(pm.list_variables("biz"))
