
import requests
import json
import time
import socket
import ssl
from typing import Dict, List, Optional
from urllib.parse import urlparse
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
            cursor.execute("SELECT * FROM email_verification_templates WHERE id = %s", (template_id,))
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
                VALUES (%s, %s, %s, %s) RETURNING id
            """, (name, service, json.dumps(api_config), json.dumps(status_mapping)))
            template_id = cursor.fetchone()['id']
            conn.commit()
            return template_id

    @staticmethod
    def update_template(template_id: int, name: str, api_config: Dict, status_mapping: Dict):
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE email_verification_templates 
                SET name = %s, api_config = %s, status_mapping = %s
                WHERE id = %s
            """, (name, json.dumps(api_config), json.dumps(status_mapping), template_id))
            conn.commit()

    @staticmethod
    def delete_template(template_id: int):
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM email_verification_templates WHERE id = %s", (template_id,))
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
        """Default status mapping for MyEmailVerifier - now using raw API statuses"""
        return {
            "valid": "Valid",
            "invalid": "Invalid", 
            "catch-all": "Catch-all",
            "unknown": "Unknown",
            "disposable": "Invalid",
            "role_based": "Valid"
        }


class DomainDnsSslIntegration:
    def __init__(self, timeout_seconds: float = 8.0):
        self.timeout_seconds = max(1.0, float(timeout_seconds or 8.0))

    def _clean_domain(self, domain: str) -> str:
        raw = str(domain or "").strip()
        if not raw:
            return ""
        if "://" not in raw:
            raw = f"http://{raw}"
        parsed = urlparse(raw)
        host = parsed.netloc or parsed.path
        host = host.split("@")[-1].split("/")[0].split(":")[0].strip().strip(".").lower()
        if host.startswith("www."):
            host = host[4:]
        return host

    def _request_url(self, scheme: str, domain: str) -> Dict:
        url = f"{scheme}://{domain}"
        try:
            response = requests.get(
                url,
                timeout=self.timeout_seconds,
                allow_redirects=True,
                stream=True,
                headers={"User-Agent": "ScrapIQ-DomainChecker/1.0"},
            )
            response.close()
            return {
                "reachable": True,
                "status_code": response.status_code,
                "final_url": response.url,
                "error": "",
            }
        except requests.exceptions.SSLError as exc:
            return {
                "reachable": False,
                "status_code": None,
                "final_url": url,
                "error": f"SSL error: {exc}",
            }
        except Exception as exc:
            return {
                "reachable": False,
                "status_code": None,
                "final_url": url,
                "error": str(exc),
            }

    def _check_certificate(self, domain: str) -> Dict:
        try:
            context = ssl.create_default_context()
            with socket.create_connection((domain, 443), timeout=self.timeout_seconds) as sock:
                with context.wrap_socket(sock, server_hostname=domain) as tls_sock:
                    cert = tls_sock.getpeercert()
                    return {
                        "valid": True,
                        "error": "",
                        "subject": cert.get("subject"),
                        "issuer": cert.get("issuer"),
                        "not_after": cert.get("notAfter"),
                    }
        except ssl.SSLError as exc:
            return {"valid": False, "error": str(exc)}
        except Exception as exc:
            return {"valid": False, "error": str(exc)}

    def check_domain(self, domain: str) -> Dict:
        clean_domain = self._clean_domain(domain)
        if not clean_domain:
            return {
                "success": False,
                "domain": domain,
                "normalized_domain": "",
                "mapped_status": "DNS Failed",
                "error": "Missing domain",
            }

        try:
            addresses = sorted({item[4][0] for item in socket.getaddrinfo(clean_domain, None)})
            dns_ok = bool(addresses)
            dns_error = ""
        except Exception as exc:
            addresses = []
            dns_ok = False
            dns_error = str(exc)

        http = self._request_url("http", clean_domain) if dns_ok else {"reachable": False, "error": dns_error}
        https = self._request_url("https", clean_domain) if dns_ok else {"reachable": False, "error": dns_error}
        cert = self._check_certificate(clean_domain) if dns_ok else {"valid": False, "error": dns_error}

        ssl_problem = bool(dns_ok and not cert.get("valid") and (http.get("reachable") or "ssl" in str(https.get("error", "")).lower()))
        if not dns_ok:
            mapped_status = "DNS Failed"
        elif https.get("reachable") and cert.get("valid"):
            mapped_status = "Domain OK"
        elif ssl_problem:
            mapped_status = "SSL Problem"
        elif http.get("reachable"):
            mapped_status = "HTTP Only"
        else:
            mapped_status = "Domain Down"

        return {
            "success": dns_ok,
            "domain": domain,
            "normalized_domain": clean_domain,
            "status": mapped_status.lower().replace(" ", "_"),
            "mapped_status": mapped_status,
            "dns_ok": dns_ok,
            "addresses": addresses,
            "http": http,
            "https": https,
            "ssl": cert,
            "error": dns_error or https.get("error") or http.get("error") or cert.get("error") or "",
            "raw_response": {
                "dns_ok": dns_ok,
                "addresses": addresses,
                "http": http,
                "https": https,
                "ssl": cert,
            },
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
                # Use the raw API status directly
                result['mapped_status'] = result['status'].title()  # Capitalize first letter (valid -> Valid)
            else:
                result['mapped_status'] = 'Unknown'
            
            results.append(result)
        
        return results

    def verify_domain(self, domain: str, template: Dict) -> Dict:
        api_config = template.get("api_config") or {}
        timeout_seconds = api_config.get("timeout_seconds", 8)
        return DomainDnsSslIntegration(timeout_seconds).check_domain(domain)
