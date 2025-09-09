
import requests
import json
import time
from typing import Dict, List, Optional
from database import get_db
from datetime import datetime, timedelta

class EmailVerificationManager:
    @staticmethod
    def get_all_templates():
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM email_verification_templates ORDER BY name")
            return [dict(row) for row in cursor.fetchall()]

    @staticmethod
    def get_template(template_id: int):
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM email_verification_templates WHERE id = ?", (template_id,))
            row = cursor.fetchone()
            if row:
                template = dict(row)
                template['api_config'] = json.loads(template['api_config'])
                template['status_mapping'] = json.loads(template['status_mapping'])
                return template
            return None

    @staticmethod
    def create_template(name: str, service: str, api_config: Dict, status_mapping: Dict):
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO email_verification_templates (name, service, api_config, status_mapping)
                VALUES (?, ?, ?, ?)
            """, (name, service, json.dumps(api_config), json.dumps(status_mapping)))
            conn.commit()
            return cursor.lastrowid

    @staticmethod
    def update_template(template_id: int, name: str, api_config: Dict, status_mapping: Dict):
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE email_verification_templates 
                SET name = ?, api_config = ?, status_mapping = ?
                WHERE id = ?
            """, (name, json.dumps(api_config), json.dumps(status_mapping), template_id))
            conn.commit()

    @staticmethod
    def delete_template(template_id: int):
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM email_verification_templates WHERE id = ?", (template_id,))
            conn.commit()

class MyEmailVerifierIntegration:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://client.myemailverifier.com/verifier/validate_single"
        self.rate_limit = 30  # 30 requests per minute per API documentation

    def verify_email(self, email: str) -> Dict:
        """Verify a single email using MyEmailVerifier API"""
        try:
            url = f"{self.base_url}/{email}/{self.api_key}"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'success': True,
                    'email': email,
                    'status': data.get('Status', 'unknown').lower(),
                    'address': data.get('Address'),
                    'catch_all': data.get('catch_all', False),
                    'disposable_domain': data.get('Disposable_Domain', False),
                    'role_based': data.get('Role_Based', False),
                    'free_domain': data.get('Free_Domain', False),
                    'greylisted': data.get('Greylisted', False),
                    'diagnosis': data.get('Diagnosis', ''),
                    'raw_response': data
                }
            else:
                return {
                    'success': False,
                    'email': email,
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'status': 'unknown'
                }
        except Exception as e:
            return {
                'success': False,
                'email': email,
                'error': str(e),
                'status': 'unknown'
            }

    def map_status(self, api_status: str, status_mapping: Dict) -> str:
        """Map API status to our internal status"""
        api_status = api_status.lower()
        return status_mapping.get(api_status, 'unknown')

    def get_default_status_mapping(self) -> Dict:
        """Default status mapping for MyEmailVerifier - using internal status values"""
        return {
            "valid": "verified",
            "invalid": "invalid", 
            "catch-all": "catch-all",
            "unknown": "unknown",
            "disposable": "invalid",
            "role_based": "verified"
        }

class EmailVerificationService:
    def __init__(self):
        self.services = {
            'myemailverifier': MyEmailVerifierIntegration
        }

    def get_integration(self, service: str, api_key: str):
        """Get integration instance for specific service"""
        if service in self.services:
            return self.services[service](api_key)
        raise ValueError(f"Unsupported email verification service: {service}")

    def verify_batch(self, emails: List[str], template: Dict, batch_delay: float = 1.0) -> List[Dict]:
        """Verify a batch of emails with rate limiting"""
        service = template['service']
        api_config = template['api_config']
        status_mapping = template['status_mapping']
        
        integration = self.get_integration(service, api_config['api_key'])
        results = []
        
        for i, email in enumerate(emails):
            if i > 0:  # Add delay between requests
                time.sleep(batch_delay)
            
            result = integration.verify_email(email)
            if result['success']:
                # Use the configured status mapping
                api_status = result['status'].lower()
                result['mapped_status'] = integration.map_status(api_status, status_mapping)
            else:
                result['mapped_status'] = 'unknown'
            
            results.append(result)
        
        return results
