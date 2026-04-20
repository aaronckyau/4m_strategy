from __future__ import annotations

from pathlib import Path


PROMPT_DIR = Path(__file__).resolve().parent / "schema"


def load_prompt_template(filename: str, **variables: str) -> str:
    prompt_path = PROMPT_DIR / filename
    template = prompt_path.read_text(encoding="utf-8")
    for key, value in variables.items():
        template = template.replace(f"{{{{{key}}}}}", value)
    return template
