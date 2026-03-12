# Sources Used For Lead Finder Improvements

This file lists the main public references used to shape the automation choices in this repo.

## Search Source and Quota
- Google Custom Search JSON API overview:
  - https://developers.google.com/custom-search/v1/overview
- Google Custom Search pricing/quota:
  - https://developers.google.com/custom-search/v1/overview#pricing
- Bing Search API retirement announcement:
  - https://learn.microsoft.com/en-au/lifecycle/announcements/bing-search-api-retirement

How used:
- Optional `--google-api-key` and `--google-cx` support in harvesting.
- Guidance that API quota is per query request, not per final lead row.
- Clarify that retired Bing Search APIs are not used; harvest fallback uses Bing HTML/RSS result pages.

## Email Quality Validation
- `email-validator` Python package:
  - https://github.com/JoshData/python-email-validator

How used:
- Syntax normalization and stricter email validity handling in `verify_emails.py`.

## Network Robustness
- `urllib3` Retry reference:
  - https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#urllib3.util.Retry

How used:
- Request retry policy with backoff in `prospect_harvest.py` and `prospect_validate.py`.

## Role Mailbox Guidance
- RFC 2142:
  - https://www.rfc-editor.org/info/rfc2142

How used:
- Role-address filtering logic (`info@`, `support@`, `admin@`, etc.) in `verify_emails.py`.

## Robots Compliance
- RFC 9309:
  - https://www.rfc-editor.org/info/rfc9309

How used:
- `prospect_validate.py` checks robots.txt before fetching target pages.
- RFC behavior implemented:
  - robots `200` -> parse and enforce
  - robots `4xx` -> treat as unavailable/allow
  - robots `5xx` or unreachable -> temporary disallow
