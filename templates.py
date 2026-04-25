import json
import re
from typing import Dict, List, Optional
from database import get_db


def _looks_like_city(value: str) -> bool:
    candidate = str(value or "").strip()
    if len(candidate) < 2:
        return False
    if any(ch.isdigit() for ch in candidate):
        return False
    return bool(re.fullmatch(r"[A-Za-z .'\-]+", candidate))


def extract_city_from_address(address: Optional[str]) -> str:
    text = str(address or "").strip()
    if not text:
        return ""

    # Typical US pattern: "Street, City, ST 12345"
    match = re.search(r",\s*([^,]+?)\s*,\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?\b", text)
    if match:
        city = match.group(1).strip()
        if _looks_like_city(city):
            return city

    parts = [part.strip() for part in text.split(",") if part.strip()]
    if len(parts) >= 3 and _looks_like_city(parts[-2]):
        return parts[-2]

    if len(parts) == 2:
        # Example fallback: "City ST 12345"
        second = parts[-1]
        second_match = re.match(r"([A-Za-z .'\-]+)\s+[A-Z]{2}\s+\d{5}(?:-\d{4})?\b", second)
        if second_match:
            city = second_match.group(1).strip()
            if _looks_like_city(city):
                return city

        if _looks_like_city(parts[0]):
            return parts[0]

    return ""


def resolve_contact_value(contact: Dict, contact_field: str) -> Optional[str]:
    if not contact_field:
        return None

    normalized_field = str(contact_field).lower().replace(' ', '_')
    if normalized_field in contact:
        value = contact[normalized_field]
        from_contact = True
    elif contact_field in contact:
        value = contact[contact_field]
        from_contact = True
    else:
        value = contact_field
        from_contact = False

    cleaned = str(value).strip() if value is not None else ""
    if cleaned:
        return cleaned

    if from_contact and normalized_field == "city":
        derived_city = extract_city_from_address(contact.get("address"))
        if derived_city:
            return derived_city
        request_city = str(contact.get("__request_city") or contact.get("request_city") or "").strip()
        if request_city:
            return request_city

    return None

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
            value = resolve_contact_value(contact, contact_field)
            if value is None:
                continue

            # Clean domain fields (both 'domain' and 'www' API fields)
            if api_field in ['domain', 'www']:
                value = self._clean_domain(value)

            transformed[api_field] = value

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

    def get_campaigns(self) -> List[Dict]:
        """Fetch campaigns available to the API key for template campaign selection."""
        import requests

        endpoints = [
            "/api/campaigns",
            "/api/campaigns/list"
        ]
        errors = []

        for endpoint in endpoints:
            url = f"{self.base_url}{endpoint}"
            try:
                response = requests.get(
                    url,
                    params={"apikey": self.api_key},
                    headers={"Accept": "application/json"},
                    timeout=30
                )
            except requests.exceptions.RequestException as e:
                errors.append(f"{endpoint}: network error {str(e)}")
                continue

            if response.status_code != 200:
                errors.append(f"{endpoint}: status {response.status_code}")
                continue

            try:
                payload = response.json()
            except ValueError:
                errors.append(f"{endpoint}: non-JSON response")
                continue

            raw_campaigns = self._extract_campaign_list(payload)
            if raw_campaigns is None:
                errors.append(f"{endpoint}: unknown response format")
                continue

            campaigns = []
            for item in raw_campaigns:
                if not isinstance(item, dict):
                    continue
                campaign_id = item.get("id") or item.get("campaignid") or item.get("campaign_id")
                if campaign_id is None:
                    continue
                name = item.get("name") or item.get("campaign_name") or f"Campaign {campaign_id}"
                status = item.get("status") or item.get("state") or ""
                campaigns.append({
                    "id": str(campaign_id),
                    "name": str(name),
                    "status": str(status)
                })

            return campaigns

        raise Exception("Unable to fetch ManyReach campaigns. " + "; ".join(errors))

    def _extract_campaign_list(self, payload) -> Optional[List]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ["campaigns", "data", "items", "results"]:
                if isinstance(payload.get(key), list):
                    return payload.get(key)
        return None


class SmartLeadIntegration:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://server.smartlead.ai/api/v1"
        # Smartlead rate limit: 10 requests / 2 seconds (~300 per minute).
        self.rate_limit = 300
        self.max_leads_per_request = 400

    def get_default_field_mapping(self):
        mapping = {
            "email": "email",
            "first_name": "firstname",
            "last_name": "lastname",
            "phone_number": "phone",
            "company_name": "business_name",
            "website": "domain",
            "location": "address",
            "linkedin_profile": "",
            "company_url": "domain"
        }
        for idx in range(1, 21):
            mapping[f"custom_{idx}"] = ""
        return mapping

    def _resolve_mapping_value(self, contact: Dict, contact_field: str) -> Optional[str]:
        return resolve_contact_value(contact, contact_field)

    def _clean_domain(self, domain: str) -> str:
        if not domain:
            return domain
        domain = domain.replace("https://", "").replace("http://", "")
        if domain.startswith("www."):
            domain = domain[4:]
        return domain.split("/")[0]

    def transform_contact(self, contact: Dict, field_mapping: Dict) -> Dict:
        transformed = {}
        custom_fields = {}

        for api_field, contact_field in field_mapping.items():
            value = self._resolve_mapping_value(contact, contact_field)
            if value is None:
                continue

            if api_field in ["website", "company_url"]:
                value = self._clean_domain(value)

            if api_field.startswith("custom_"):
                custom_fields[api_field] = value
            else:
                transformed[api_field] = value

        if custom_fields:
            transformed["custom_fields"] = custom_fields

        return transformed

    def validate_contact(self, transformed_contact: Dict) -> bool:
        return bool(transformed_contact.get("email"))

    def get_campaign(self, campaign_id: str) -> Dict:
        import requests

        url = f"{self.base_url}/campaigns/{campaign_id}"
        try:
            response = requests.get(url, params={"api_key": self.api_key}, timeout=30)
            if response.status_code != 200:
                raise Exception(f"Smartlead campaign lookup failed with status {response.status_code}: {response.text}")
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Smartlead campaign lookup network error: {str(e)}")

    def get_campaigns(self, active_only: bool = False) -> List[Dict]:
        """Fetch campaigns available to the Smartlead API key."""
        import requests

        url = f"{self.base_url}/campaigns"
        try:
            response = requests.get(url, params={"api_key": self.api_key}, timeout=30)
            if response.status_code != 200:
                raise Exception(f"Smartlead campaign list failed with status {response.status_code}: {response.text}")
            payload = response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Smartlead campaign list network error: {str(e)}")

        raw_campaigns = self._extract_campaign_list(payload)
        if raw_campaigns is None:
            raise Exception("Smartlead campaign list returned unknown response format")

        campaigns = []
        for item in raw_campaigns:
            if not isinstance(item, dict):
                continue
            campaign_id = item.get("id") or item.get("campaign_id")
            if campaign_id is None:
                continue
            status = str(item.get("status", ""))
            if active_only and status.upper() not in ["ACTIVE", "RUNNING"]:
                continue
            name = item.get("name") or item.get("campaign_name") or f"Campaign {campaign_id}"
            campaigns.append({
                "id": str(campaign_id),
                "name": str(name),
                "status": status
            })

        return campaigns

    def _extract_campaign_list(self, payload) -> Optional[List]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ["data", "campaigns", "items", "results"]:
                if isinstance(payload.get(key), list):
                    return payload.get(key)
        return None

    def _sanitize_settings(self, settings: Optional[Dict]) -> Dict:
        """Drop legacy/unsupported settings keys before sending to Smartlead."""
        if not isinstance(settings, dict):
            return {}

        sanitized = {}
        for key, value in settings.items():
            normalized_key = str(key).strip()
            if not normalized_key:
                continue
            # Legacy key from older integration payloads; Smartlead rejects it.
            if normalized_key == "ignore_duplicate_contacts_within_campaign":
                continue
            sanitized[normalized_key] = value
        return sanitized

    def _strip_disallowed_settings_from_payload(self, payload: Dict, response) -> Optional[Dict]:
        """Parse Smartlead validation errors and remove rejected settings keys."""
        if response.status_code != 400:
            return None

        try:
            response_data = response.json()
        except ValueError:
            return None

        validation = response_data.get("validation", {})
        keys = validation.get("keys", [])
        if not isinstance(keys, list):
            return None

        current_settings = payload.get("settings")
        if not isinstance(current_settings, dict):
            return None

        cleaned_settings = dict(current_settings)
        removed_any = False
        for key_path in keys:
            key_path = str(key_path)
            if key_path.startswith("settings."):
                setting_key = key_path.split(".", 1)[1]
                if setting_key in cleaned_settings:
                    cleaned_settings.pop(setting_key, None)
                    removed_any = True

        if not removed_any:
            return None

        retried_payload = {
            "lead_list": payload.get("lead_list", [])
        }
        if cleaned_settings:
            retried_payload["settings"] = cleaned_settings
        return retried_payload

    def ensure_active_campaign(self, campaign_id: str):
        campaign = self.get_campaign(campaign_id)
        status = str(campaign.get("status", "")).upper()
        if status != "ACTIVE":
            raise Exception(f"Smartlead campaign {campaign_id} is not active (status={status or 'UNKNOWN'})")

    def export_to_smartlead_bulk(self, campaign_id: str, leads: List[Dict], settings: Optional[Dict] = None) -> Dict:
        import requests

        if len(leads) > self.max_leads_per_request:
            raise Exception(f"Smartlead supports max {self.max_leads_per_request} leads per request")

        payload = {
            "lead_list": leads
        }
        sanitized_settings = self._sanitize_settings(settings)
        if sanitized_settings:
            payload["settings"] = sanitized_settings

        url = f"{self.base_url}/campaigns/{campaign_id}/leads"

        try:
            response = requests.post(
                url,
                params={"api_key": self.api_key},
                json=payload,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=30
            )
            if response.status_code == 200:
                return response.json()

            retried_payload = self._strip_disallowed_settings_from_payload(payload, response)
            if retried_payload is not None:
                retry_response = requests.post(
                    url,
                    params={"api_key": self.api_key},
                    json=retried_payload,
                    headers={"Content-Type": "application/json", "Accept": "application/json"},
                    timeout=30
                )
                if retry_response.status_code == 200:
                    return retry_response.json()
                raise Exception(f"Smartlead API returned status {retry_response.status_code}: {retry_response.text}")

            raise Exception(f"Smartlead API returned status {response.status_code}: {response.text}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Smartlead network error: {str(e)}")


class SendReadIntegration:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://app.sendread.co"
        self.rate_limit = 120

    def get_default_field_mapping(self):
        return {
            "email": "email",
            "firstName": "firstname",
            "company": "business_name",
            "city": "city",
            "phone": "phone",
            "website": "domain",
            "industry": "industry",
            "facebook": "facebook",
            "linkedin": "company_social",
            "tags": "tags_import",
            "custom1": "custom_1",
            "custom2": "custom_2",
            "custom3": "custom_3",
            "custom4": "custom_4",
            "custom5": "custom_5"
        }

    def _auth_headers(self) -> Dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "x-api-key": self.api_key,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def _resolve_mapping_value(self, contact: Dict, contact_field: str) -> Optional[str]:
        return resolve_contact_value(contact, contact_field)

    def _clean_website(self, website: str) -> str:
        if not website:
            return website
        website = website.strip()
        if website.startswith("http://") or website.startswith("https://"):
            return website
        return f"https://{website}"

    def transform_contact(self, contact: Dict, field_mapping: Dict) -> Dict:
        transformed = {}

        for api_field, contact_field in field_mapping.items():
            value = self._resolve_mapping_value(contact, contact_field)
            if value is None:
                continue

            if api_field == "website":
                value = self._clean_website(value)

            transformed[api_field] = value

        return transformed

    def validate_contact(self, transformed_contact: Dict) -> bool:
        return bool((transformed_contact.get("email") or "").strip())

    def _extract_list(self, payload) -> Optional[List]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ["data", "items", "results", "campaigns", "lists"]:
                if isinstance(payload.get(key), list):
                    return payload.get(key)
        return None

    def get_campaigns(self) -> List[Dict]:
        import requests

        url = f"{self.base_url}/api/public/campaigns"
        try:
            response = requests.get(url, headers=self._auth_headers(), timeout=30)
            if response.status_code != 200:
                raise Exception(f"SendRead campaign list failed with status {response.status_code}: {response.text}")
            payload = response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"SendRead campaign list network error: {str(e)}")

        raw_campaigns = self._extract_list(payload)
        if raw_campaigns is None:
            raise Exception("SendRead campaign list returned unknown response format")

        campaigns = []
        for item in raw_campaigns:
            if not isinstance(item, dict):
                continue
            campaign_id = item.get("id") or item.get("campaign_id")
            if campaign_id is None:
                continue
            name = item.get("name") or item.get("campaign_name") or f"Campaign {campaign_id}"
            status = item.get("status") or ""
            campaigns.append({
                "id": str(campaign_id),
                "name": str(name),
                "status": str(status)
            })
        return campaigns

    def get_ab_test_lists(self) -> List[Dict]:
        import requests

        url = f"{self.base_url}/api/public/ab-test-lists"
        try:
            response = requests.get(url, headers=self._auth_headers(), timeout=30)
            if response.status_code != 200:
                raise Exception(f"SendRead AB test list fetch failed with status {response.status_code}: {response.text}")
            payload = response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"SendRead AB test list fetch network error: {str(e)}")

        raw_lists = self._extract_list(payload)
        if raw_lists is None:
            raise Exception("SendRead AB test list response format unknown")

        lists = []
        for item in raw_lists:
            if not isinstance(item, dict):
                continue
            list_id = item.get("id") or item.get("list_id") or item.get("ab_list_id")
            if list_id is None:
                continue
            name = item.get("name") or item.get("list_name") or f"AB List {list_id}"
            status = item.get("status") or ""
            lists.append({
                "id": str(list_id),
                "name": str(name),
                "status": str(status)
            })
        return lists

    def export_to_campaign(self, campaign_id: str, leads: List[Dict]) -> Dict:
        import requests

        url = f"{self.base_url}/api/public/campaigns/{campaign_id}/leads"
        payload = {"leads": leads}
        try:
            response = requests.post(url, headers=self._auth_headers(), json=payload, timeout=30)
            if response.status_code not in [200, 201]:
                raise Exception(f"SendRead campaign export failed with status {response.status_code}: {response.text}")
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"SendRead campaign export network error: {str(e)}")

    def export_to_ab_test_list(self, ab_list_id: str, leads: List[Dict]) -> Dict:
        import requests

        url = f"{self.base_url}/api/public/ab-test-lists/{ab_list_id}/leads"
        payload = {"leads": leads}
        try:
            response = requests.post(url, headers=self._auth_headers(), json=payload, timeout=30)
            if response.status_code not in [200, 201]:
                raise Exception(f"SendRead AB list export failed with status {response.status_code}: {response.text}")
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"SendRead AB list export network error: {str(e)}")
