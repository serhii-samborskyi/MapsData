
import json
from typing import Dict, List, Optional
from database import get_db

class TemplateManager:
    @staticmethod
    def get_all_templates():
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM export_templates ORDER BY name")
            return [dict(row) for row in cursor.fetchall()]
    
    @staticmethod
    def get_template(template_id: int):
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM export_templates WHERE id = ?", (template_id,))
            row = cursor.fetchone()
            if row:
                template = dict(row)
                template['field_mappings'] = json.loads(template['field_mappings'])
                template['api_config'] = json.loads(template['api_config'])
                return template
            return None
    
    @staticmethod
    def create_template(name: str, service: str, field_mappings: Dict, api_config: Dict):
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO export_templates (name, service, field_mappings, api_config)
                VALUES (?, ?, ?, ?)
            """, (name, service, json.dumps(field_mappings), json.dumps(api_config)))
            conn.commit()
            return cursor.lastrowid
    
    @staticmethod
    def update_template(template_id: int, name: str, field_mappings: Dict, api_config: Dict, service: str = None):
        with get_db() as conn:
            cursor = conn.cursor()
            if service:
                cursor.execute("""
                    UPDATE export_templates 
                    SET name = ?, service = ?, field_mappings = ?, api_config = ?
                    WHERE id = ?
                """, (name, service, json.dumps(field_mappings), json.dumps(api_config), template_id))
            else:
                cursor.execute("""
                    UPDATE export_templates 
                    SET name = ?, field_mappings = ?, api_config = ?
                    WHERE id = ?
                """, (name, json.dumps(field_mappings), json.dumps(api_config), template_id))
            conn.commit()
    
    @staticmethod
    def delete_template(template_id: int):
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM export_templates WHERE id = ?", (template_id,))
            conn.commit()

class ManyReachIntegration:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://app.manyreach.com"
        self.rate_limit = 60  # requests per minute
        
    def get_default_field_mapping(self):
        return {
            "email": "email",
            "company": "business_name", 
            "www": "domain",
            "phone": "phone",
            "industry": "category",
            "firstname": "custom_1",
            "lastname": "custom_2",
            "custom_3": "",
            "custom_4": "",
            "custom_5": "",
            "custom_6": "",
            "custom_7": "",
            "custom_8": "",
            "custom_9": "",
            "custom_10": "",
            "custom_11": "",
            "custom_12": "",
            "custom_13": "",
            "custom_14": "",
            "custom_15": "",
            "custom_16": "",
            "custom_17": "",
            "custom_18": "",
            "custom_19": "",
            "custom_20": ""
        }
    
    def transform_contact(self, contact: Dict, field_mapping: Dict, manyreach_campaign_id: str = None) -> Dict:
        """Transform contact data according to field mapping"""
        transformed = {}
        
        for api_field, contact_field in field_mapping.items():
            if contact_field and contact_field in contact:
                value = contact[contact_field]
                if value is not None and str(value).strip():
                    transformed[api_field] = str(value).strip()
        
        # Add campaignid if provided
        if manyreach_campaign_id:
            transformed['campaignid'] = manyreach_campaign_id
        
        return transformed
    
    def validate_contact(self, contact: Dict) -> bool:
        """Validate that contact has required fields"""
        return bool(contact.get('email'))
