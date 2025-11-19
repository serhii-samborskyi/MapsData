# ScrapIQ - Google Maps Lead Generation Platform

## Overview

ScrapIQ is a lead generation platform that scrapes Google Maps business data and manages outreach campaigns. The system allows users to create search campaigns, collect business contact information through a Chrome extension, verify email addresses, and export data to CRM systems. It's built with FastAPI and PostgreSQL, featuring a web-based management interface and API endpoints for both the Chrome extension and third-party integrations.

## Recent Changes

**November 19, 2025 - PostgreSQL Migration Completed:**
- Migrated from SQLite to PostgreSQL for persistent data storage in Replit environment
- Fixed all SQL parameter placeholders from SQLite syntax (`?`) to PostgreSQL syntax (`%s`)
- Fixed all `LIKE` operators to use case-insensitive `ILIKE` for keyword filtering
- Converted `cursor.lastrowid` usage to PostgreSQL's `RETURNING id` clause
- Verified all campaign management functions working correctly:
  - Campaign creation, duplication, deletion
  - Contact management (add, remove, deduplicate, filter)
  - Exclude contacts from other campaigns
  - Email verification integration
  - CRM export functionality

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture

**Technology Stack:**
- **Templating:** Jinja2 templates for server-side rendering
- **Styling:** Tailwind CSS (CDN-based) with PostCSS configuration
- **Font:** Google Fonts (Inter typeface)

**Design Pattern:**
- Server-rendered HTML with progressive enhancement
- Partial template rendering for dynamic content updates
- RESTful form submissions with standard HTTP methods

**Key UI Components:**
- Campaign management dashboard (index.html)
- Email verification interface (verify.html)
- Export/CRM integration manager (export.html)
- API documentation page (docs.html)

**Rationale:** Server-side rendering chosen for simplicity and SEO benefits. Tailwind CSS enables rapid UI development without custom CSS compilation in production.

### Backend Architecture

**Framework:** FastAPI
- Chosen for automatic API documentation, type validation, and async support
- Provides both HTML views and RESTful API endpoints
- Built-in OpenAPI/Swagger documentation

**Application Structure:**
- `main.py` - Primary application entry point with route handlers
- `database.py` - Database connection management with context managers
- `templates.py` - CRM export template management (ManyReach integration)
- `email_verification.py` - Email verification service integration (MyEmailVerifier)

**Key Design Decisions:**

1. **Modular Service Layer:** Separate modules for templates and email verification allow independent scaling and testing
2. **Context Manager Pattern:** Database connections use context managers for automatic cleanup and connection pooling
3. **Template-Based Configuration:** Export and verification services use stored templates for reusability across campaigns

### Data Storage

**Database:** PostgreSQL (via psycopg2)

**Schema Design:**
- `search_campaigns` - Campaign metadata and status tracking
- `requests` - Individual search queries linked to campaigns
- `contacts` - Scraped business data with foreign key to campaigns
- `export_templates` - Reusable CRM export configurations (JSON fields)
- `email_verification_templates` - Reusable verification service configurations (JSON fields)

**Key Patterns:**
- Foreign key relationships enforce referential integrity
- JSON columns store flexible configuration data (field mappings, API configs)
- Status enums track workflow states (active/paused/completed, pending/reserved/completed)
- RealDictCursor for convenient dictionary-based result access

**Rationale:** PostgreSQL chosen for ACID compliance, JSON support, and production-grade reliability. JSON columns provide flexibility for template configurations without schema migrations.

### Chrome Extension Integration

**Architecture:**
- Manifest V3 extension with service worker background script
- Content script injected into Google Maps pages
- Message passing between popup, background worker, and content script

**Workflow:**
1. Extension polls API for available requests from active campaigns
2. Opens Google Maps search URL in new tab
3. Content script scrapes business listings via DOM manipulation
4. Background worker submits scraped data back to API
5. Marks requests as completed and continues until campaign finished

**API Endpoints for Extension:**
- `GET /api/campaigns/active` - Fetch active campaigns
- `GET /api/campaign/{name}/requests` - Get pending search requests
- `POST /api/request/{id}/status` - Update request status
- `POST /api/campaign/{id}/contacts` - Submit scraped contacts

### Email Verification System

**Service Integration:**
- Template-based configuration for multiple verification providers
- Currently supports MyEmailVerifier API
- Status mapping system translates provider responses to normalized statuses

**Features:**
- Batch verification support
- Template storage for API credentials and field mappings
- Status normalization (Valid, Invalid, Catch-all, Unknown)
- Campaign-level verification management

**Design Choice:** Template system allows adding new verification providers without code changes, just configuration.

### CRM Export System

**Integration Pattern:**
- Template-based field mapping between ScrapIQ contacts and CRM fields
- Currently supports ManyReach CRM
- Configurable API endpoints and authentication

**Architecture:**
- `TemplateManager` class handles CRUD operations for export templates
- Field mapping stored as JSON for flexibility
- API configuration includes endpoints, auth tokens, and request formatting

**Extensibility:** New CRM integrations can be added by implementing new integration classes following the ManyReach pattern.

## External Dependencies

### Third-Party Services

**MyEmailVerifier API:**
- Purpose: Email address validation
- Integration: REST API with API key authentication
- Endpoint: `https://client.myemailverifier.com/verifier/validate_single/{email}/{api_key}`
- Response fields: Status, Disposable_Domain, Role_Based, Free_Domain, Greylisted, Diagnosis
- Rate limit: 30 requests per minute

**ManyReach CRM:**
- Purpose: Contact export and management
- Integration: REST API (configuration stored in templates)
- Configurable field mapping from ScrapIQ to ManyReach schema

**Google Maps:**
- Purpose: Source for business data scraping
- Integration: Chrome extension content script with DOM scraping
- No direct API usage (scraping public search results)

### Python Dependencies

**Core Framework:**
- `fastapi==0.115.0` - Web framework
- `uvicorn==0.30.6` - ASGI server
- `jinja2==3.1.4` - Template engine
- `python-multipart==0.0.9` - Form data parsing
- `aiofiles==24.1.0` - Async file operations

**Database:**
- `psycopg2` - PostgreSQL adapter with RealDictCursor support

**HTTP Client:**
- `requests` - For external API calls to verification and CRM services

### Frontend Dependencies

**CDN Resources:**
- Tailwind CSS (via CDN in templates)
- Google Fonts (Inter typeface)

**Build Tools:**
- `tailwindcss` - CSS framework (PostCSS plugin)
- `autoprefixer` - CSS vendor prefixing

**Configuration:**
- `tailwind.config.js` - Configured to scan templates directory
- `postcss.config.js` - PostCSS plugin configuration

### Environment Configuration

**Required Environment Variables:**
- `DATABASE_URL` - PostgreSQL connection string

**API Keys (stored in templates):**
- Email verification service API keys
- CRM integration authentication tokens