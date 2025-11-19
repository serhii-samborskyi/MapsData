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
            cursor.execute("SELECT * FROM export_templates WHERE id = %s", (template_id,))
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
                VALUES (%s, %s, %s, %s) RETURNING id
            """, (name, service, json.dumps(field_mappings), json.dumps(api_config)))
            template_id = cursor.fetchone()['id']
            conn.commit()
            return template_id

    @staticmethod
    def update_template(template_id: int, name: str, field_mappings: Dict, api_config: Dict, service: str = None):
        with get_db() as conn:
            cursor = conn.cursor()
            if service:
                cursor.execute("""
                    UPDATE export_templates 
                    SET name = %s, service = %s, field_mappings = %s, api_config = %s
                    WHERE id = %s
                """, (name, service, json.dumps(field_mappings), json.dumps(api_config), template_id))
            else:
                cursor.execute("""
                    UPDATE export_templates 
                    SET name = %s, field_mappings = %s, api_config = %s
                    WHERE id = %s
                """, (name, json.dumps(field_mappings), json.dumps(api_config), template_id))
            conn.commit()

    @staticmethod
    def delete_template(template_id: int):
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM export_templates WHERE id = %s", (template_id,))
            conn.commit()

class ManyReachIntegration:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://app.manyreach.com"
        self.rate_limit = 60  # requests per minute

    def get_default_field_mapping(self):
        return {
            "email": "email",
            "fullName": "",
            "industry": "category",
            "city": "",
            "www": "domain",
            "phone": "phone",
            "firstname": "",
            "lastname": "",
            "company": "business_name",
            "country": "",
            "domain": "domain",
            "companySocial": "",
            "companySize": "",
            "personalJobPosition": "",
            "personalProspectLocation": "",
            "personalUserSocial": "",
            "screenshot": "",
            "logo": "",
            "state": "",
            "icebreaker": "",
            "timeZoneOffsetMin": "",
            "notes": "",
            "tagsImport": "",
            "custom_1": "",
            "custom_2": "",
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

    def transform_contact(self, contact: Dict, field_mapping: Dict, manyreach_campaign_id: str = None, new_list_name: str = None) -> Dict:
        """Transform contact data according to field mapping"""
        transformed = {}

        for api_field, contact_field in field_mapping.items():
            if contact_field:
                # Check if it's a database field or a custom value
                if contact_field in contact:
                    # Database field - use contact data
                    value = contact[contact_field]
                    if value is not None and str(value).strip():
                        cleaned_value = str(value).strip()

                        # Clean domain fields (both 'domain' and 'www' API fields)
                        if api_field in ['domain', 'www']:
                            cleaned_value = self._clean_domain(cleaned_value)

                        transformed[api_field] = cleaned_value
                else:
                    # Custom value - use as-is (user-entered text)
                    cleaned_value = str(contact_field).strip()
                    if cleaned_value:
                        # Clean domain fields even for custom values
                        if api_field in ['domain', 'www']:
                            cleaned_value = self._clean_domain(cleaned_value)

                        transformed[api_field] = cleaned_value

        # Add campaignid if provided
        if manyreach_campaign_id:
            transformed['campaignid'] = manyreach_campaign_id

        return transformed

    def _clean_domain(self, domain: str) -> str:
        """Clean domain by removing protocol and www prefix"""
        if not domain:
            return domain

        # Remove protocols
        domain = domain.replace("https://", "").replace("http://", "")

        # Remove www prefix
        if domain.startswith("www."):
            domain = domain[4:]

        # Remove trailing slash and path
        domain = domain.split("/")[0]

        return domain

    def validate_contact(self, contact: Dict) -> bool:
        """Validate that contact has required fields"""
        return bool(contact.get('email'))

    def export_to_manyreach_bulk(self, bulk_data: Dict) -> Dict:
        """Make actual API call to ManyReach bulk endpoint"""
        import requests

        # Extract API key, campaign ID, and newListName from bulk_data
        api_key = bulk_data.pop('apikey', '')
        campaign_id = bulk_data.pop('campaignid', '')
        new_list_name = bulk_data.pop('newListName', '')

        # Build URL with query parameters
        url = f"{self.base_url}/api/campaigns/prospects/add/bulk?apikey={api_key}&campaignid={campaign_id}"
        
        # Add newListName parameter if provided
        if new_list_name:
            url += f"&newListName={new_list_name}"

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'apiKey': api_key  # Add apiKey to headers as shown in your example
        }

        # Send only the prospects array as the body
        prospects_data = bulk_data.get('prospects', [])

        try:
            response = requests.post(url, json=prospects_data, headers=headers, timeout=30)

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"API returned status {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            raise Exception(f"Network error: {str(e)}")