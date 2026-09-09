import json

from remote_mcp.drafts import _public_status
from remote_mcp.public import export_preview, template_metadata


def test_stream_status_has_bounded_progress_and_no_private_fields():
    raw = {
        "status": "running",
        "execution_mode": "streaming",
        "exported_contacts": 8,
        "stream_progress": [
            {
                "step_type": "export",
                "step_order": 5,
                "running": 2,
                "completed": 8,
                "uncertain": 1,
                "failed": -1,
                "total": 2**60,
                "retry": True,
                "api_key": "SECRET",
                "logs": ["SECRET"],
            }
        ]
        * 100,
    }
    result = _public_status(raw)
    assert result["execution_mode"] == "streaming"
    assert result["exported_contacts"] == 8
    assert len(result["stream_progress"]) == 32
    assert result["stream_progress"][0] == {
        "step_type": "export",
        "step_order": 5,
        "running": 2,
        "completed": 8,
        "uncertain": 1,
    }
    assert "SECRET" not in json.dumps(result)


def test_template_type_metadata_is_visible_without_api_config():
    result = template_metadata(
        {
            "id": 1,
            "name": "Meta source",
            "source_type": "http_api",
            "service": "meta_ads",
            "execution_mode": "streaming",
            "api_config": {"url": "https://secret.invalid/?key=SECRET"},
        },
        "source",
    )
    assert result["source_type"] == "http_api"
    assert result["service"] == "meta_ads"
    assert result["execution_mode"] == "streaming"
    assert "SECRET" not in json.dumps(result)


def test_export_preview_shows_default_destination_filters_and_safe_mapping():
    snapshot = {
        "id": 3,
        "name": "Export",
        "service": "sendread_list",
        "api_config": {"sendread_target_id": "default-list", "api_key": "SECRET"},
        "field_mappings": {
            "email": "email",
            "city": "source_data.city",
            "custom_1": "SECRET",
            "custom_2": "https://provider.invalid/?api_key=SECRET",
        },
    }
    plan = {
        "steps": [
            {
                "type": "export",
                "enabled": True,
                "config": {
                    "template_snapshot": snapshot,
                    "export_valid_only": True,
                    "filters": {"exclude_public_emails": True},
                    "require_confirmation": True,
                },
            }
        ]
    }
    result = export_preview(plan)
    assert result["destination"]["target_id"] == "default-list"
    assert result["destination"]["target_type"] == "ab_test_list"
    assert result["filters"]["export_valid_only"] is True
    assert result["filters"]["exclude_public_emails"] is True
    assert result["require_confirmation"] is True
    assert result["field_mappings"]["email"] == "email"
    assert result["field_mappings"]["city"] == "source_data.city"
    assert "SECRET" not in json.dumps(result)
    assert "provider.invalid" not in json.dumps(result)
    plan["steps"][0]["config"]["sendread_ab_list_id"] = "override-list"
    assert export_preview(plan)["destination"]["target_id"] == "override-list"
