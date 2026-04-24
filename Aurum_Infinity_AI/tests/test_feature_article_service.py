from __future__ import annotations

import json

from services import feature_article_service


class TestFeatureArticleService:
    def test_save_feature_article_normalizes_mojibake_html_to_utf8(self, tmp_path, monkeypatch):
        feature_dir = tmp_path / "News_features"
        article_dir = feature_dir / "articles"
        image_dir = feature_dir / "images"
        manifest_path = feature_dir / "manifest.json"
        article_dir.mkdir(parents=True)
        image_dir.mkdir(parents=True)
        manifest_path.write_text("[]", encoding="utf-8")

        monkeypatch.setattr(feature_article_service, "_FEATURE_DIR", feature_dir)
        monkeypatch.setattr(feature_article_service, "_ARTICLE_DIR", article_dir)
        monkeypatch.setattr(feature_article_service, "_IMAGE_DIR", image_dir)
        monkeypatch.setattr(feature_article_service, "_MANIFEST_PATH", manifest_path)

        original = "<!doctype html><html><head><title>美伊戰爭全球經濟影響研究報告</title></head><body>返回專題文章</body></html>"
        mojibake = original.encode("utf-8").decode("latin1").encode("utf-8")

        feature = feature_article_service.save_feature_article(
            slug="iran-report",
            title="測試專題",
            summary="摘要",
            date="2026-04-23",
            tags=["宏觀"],
            source="4M 專題",
            html_bytes=mojibake,
        )

        saved_html = feature["path"].read_text(encoding="utf-8")
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        assert "美伊戰爭全球經濟影響研究報告" in saved_html
        assert "返回專題文章" in saved_html
        assert '<meta charset="utf-8"' in saved_html.lower()
        assert manifest_payload[0]["html_file"] == "iran-report.html"
