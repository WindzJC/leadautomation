#!/usr/bin/env python3
"""
Verify lead emails with syntax + DNS checks, and optional SMTP probing.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import smtplib
import socket
import time
from typing import Dict, List, Optional, Tuple

try:
    from email_validator import EmailNotValidError, validate_email

    HAS_EMAIL_VALIDATOR = True
except Exception:
    HAS_EMAIL_VALIDATOR = False

try:
    import dns.exception
    import dns.resolver

    HAS_DNSPYTHON = True
except Exception:
    HAS_DNSPYTHON = False

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,24}$")

RESULT_COLUMNS = [
    "NormalizedEmail",
    "IsRoleAddress",
    "TypoFixed",
    "VerificationStatus",
    "VerificationReason",
    "HasMX",
    "HasAorAAAA",
    "CheckedAtUTC",
]

SOFT_ROLE_LOCALPARTS = {
    "admin",
    "support",
    "info",
    "sales",
    "billing",
    "contact",
    "help",
    "team",
    "office",
    "hello",
    "marketing",
    "privacy",
    "legal",
}

HARD_BLOCK_ROLE_LOCALPARTS = {
    "abuse",
    "postmaster",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
}

COMMON_DOMAIN_FIXES = {
    "gamil.com": "gmail.com",
    "gmial.com": "gmail.com",
    "gmai.com": "gmail.com",
    "gmal.com": "gmail.com",
    "hotnail.com": "hotmail.com",
    "hotmai.com": "hotmail.com",
    "outllok.com": "outlook.com",
    "outlok.com": "outlook.com",
    "yahho.com": "yahoo.com",
    "yaho.com": "yahoo.com",
    "yhoo.com": "yahoo.com",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify emails from prospect CSV.")
    parser.add_argument("--input", default="final_prospects.csv", help="Input CSV with AuthorEmail column.")
    parser.add_argument("--output", default="final_prospects_verified.csv", help="Output verified CSV.")
    parser.add_argument("--email-column", default="AuthorEmail", help="Email column name.")
    parser.add_argument("--timeout", type=float, default=8.0, help="DNS/SMTP timeout seconds.")
    parser.add_argument("--sleep", type=float, default=0.0, help="Pause between checks.")
    parser.add_argument("--max-rows", type=int, default=0, help="Optional cap for testing.")
    parser.add_argument("--smtp-probe", action="store_true", help="Attempt RCPT probe via SMTP.")
    parser.add_argument("--smtp-from", default="verify@localhost", help="MAIL FROM used in SMTP probe.")
    parser.add_argument("--allow-role", action="store_true", help="Allow role addresses (info@, support@, etc.).")
    return parser.parse_args()


def normalize_email(value: str) -> str:
    email = (value or "").strip().lower()
    email = email.replace("mailto:", "", 1).split("?", 1)[0].strip(" \t\r\n<>\"'.,;:()[]{}")
    return email


def is_syntax_valid(email: str) -> bool:
    return bool(EMAIL_RE.fullmatch(email))


def normalize_syntax(email: str) -> Tuple[str, Optional[str]]:
    if HAS_EMAIL_VALIDATOR:
        try:
            info = validate_email(email, check_deliverability=False)
            return info.normalized, None
        except EmailNotValidError as exc:
            return "", str(exc)
    return (email, None) if is_syntax_valid(email) else ("", "invalid_syntax")


def domain_from_email(email: str) -> str:
    _, _, domain = email.partition("@")
    return domain.strip().lower()


def local_from_email(email: str) -> str:
    local, _, _ = email.partition("@")
    return local.strip().lower()


def is_role_address(email: str) -> bool:
    local = local_from_email(email)
    all_roles = SOFT_ROLE_LOCALPARTS | HARD_BLOCK_ROLE_LOCALPARTS
    if local in all_roles:
        return True
    for role in all_roles:
        if local.startswith(role) and len(local) > len(role):
            sep = local[len(role)]
            if sep in {".", "-", "_", "+"}:
                return True
    return False


def is_hard_block_role(email: str) -> bool:
    local = local_from_email(email)
    if local in HARD_BLOCK_ROLE_LOCALPARTS:
        return True
    for role in HARD_BLOCK_ROLE_LOCALPARTS:
        if local.startswith(role) and len(local) > len(role):
            sep = local[len(role)]
            if sep in {".", "-", "_", "+"}:
                return True
    return False


def resolve_domain_records(domain: str, timeout: float) -> Tuple[bool, bool, List[str]]:
    """
    Returns:
      has_mx, has_a_or_aaaa, mx_hosts_sorted
    """
    if not domain:
        return False, False, []

    if HAS_DNSPYTHON:
        resolver = dns.resolver.Resolver()
        resolver.timeout = timeout
        resolver.lifetime = timeout

        mx_hosts: List[Tuple[int, str]] = []
        has_mx = False
        has_a_or_aaaa = False

        try:
            answers = resolver.resolve(domain, "MX")
            for answer in answers:
                pref = int(getattr(answer, "preference", 0))
                host = str(getattr(answer, "exchange", "")).rstrip(".").lower()
                if host:
                    mx_hosts.append((pref, host))
            has_mx = len(mx_hosts) > 0
        except (dns.exception.DNSException, Exception):
            has_mx = False

        try:
            resolver.resolve(domain, "A")
            has_a_or_aaaa = True
        except (dns.exception.DNSException, Exception):
            pass

        if not has_a_or_aaaa:
            try:
                resolver.resolve(domain, "AAAA")
                has_a_or_aaaa = True
            except (dns.exception.DNSException, Exception):
                pass

        mx_hosts.sort(key=lambda item: item[0])
        return has_mx, has_a_or_aaaa, [host for _, host in mx_hosts]

    # Fallback when dnspython is unavailable.
    try:
        socket.getaddrinfo(domain, None)
        return False, True, []
    except socket.gaierror:
        return False, False, []


def smtp_probe(email: str, mx_hosts: List[str], timeout: float, smtp_from: str) -> Tuple[str, str]:
    if not mx_hosts:
        return "unknown", "no_mx_host_for_smtp_probe"

    target_mx = mx_hosts[0]
    try:
        with smtplib.SMTP(target_mx, 25, timeout=timeout) as server:
            server.ehlo_or_helo_if_needed()
            server.mail(smtp_from)
            code, msg = server.rcpt(email)
            message = (msg.decode("utf-8", errors="ignore") if isinstance(msg, bytes) else str(msg or "")).strip()

            if code in (250, 251):
                return "deliverable", f"smtp_accept:{code}:{message}"
            if code in (450, 451, 452):
                return "unknown", f"smtp_tempfail:{code}:{message}"
            if code in (550, 551, 552, 553, 554):
                return "undeliverable", f"smtp_reject:{code}:{message}"
            return "unknown", f"smtp_code:{code}:{message}"
    except Exception as exc:
        return "unknown", f"smtp_error:{type(exc).__name__}"


def verify_email(
    raw_email: str,
    timeout: float,
    do_smtp_probe: bool,
    smtp_from: str,
    allow_role: bool,
    domain_cache: Dict[str, Tuple[bool, bool, List[str]]],
) -> Dict[str, str]:
    checked_at = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    email = normalize_email(raw_email)
    typo_fixed = False

    if not email:
        return {
            "NormalizedEmail": "",
            "IsRoleAddress": "False",
            "TypoFixed": "False",
            "VerificationStatus": "missing_email",
            "VerificationReason": "empty",
            "HasMX": "False",
            "HasAorAAAA": "False",
            "CheckedAtUTC": checked_at,
        }

    normalized_email, syntax_err = normalize_syntax(email)
    if not normalized_email:
        return {
            "NormalizedEmail": email,
            "IsRoleAddress": "False",
            "TypoFixed": "False",
            "VerificationStatus": "invalid",
            "VerificationReason": (syntax_err or "invalid_syntax"),
            "HasMX": "False",
            "HasAorAAAA": "False",
            "CheckedAtUTC": checked_at,
        }
    email = normalized_email

    domain = domain_from_email(email)
    if domain in COMMON_DOMAIN_FIXES:
        fixed_domain = COMMON_DOMAIN_FIXES[domain]
        email = f"{local_from_email(email)}@{fixed_domain}"
        domain = fixed_domain
        typo_fixed = True

    role_addr = is_role_address(email)
    hard_role = is_hard_block_role(email)
    soft_role = role_addr and not hard_role
    if hard_role and not allow_role:
        return {
            "NormalizedEmail": email,
            "IsRoleAddress": "True",
            "TypoFixed": str(typo_fixed),
            "VerificationStatus": "blocked_role",
            "VerificationReason": "hard_role_address_blocked",
            "HasMX": "False",
            "HasAorAAAA": "False",
            "CheckedAtUTC": checked_at,
        }

    if domain not in domain_cache:
        domain_cache[domain] = resolve_domain_records(domain, timeout=timeout)
    has_mx, has_a_aaaa, mx_hosts = domain_cache[domain]

    if not has_mx and not has_a_aaaa:
        return {
            "NormalizedEmail": email,
            "IsRoleAddress": str(role_addr),
            "TypoFixed": str(typo_fixed),
            "VerificationStatus": "undeliverable",
            "VerificationReason": "domain_not_resolvable",
            "HasMX": "False",
            "HasAorAAAA": "False",
            "CheckedAtUTC": checked_at,
        }

    if not has_mx:
        return {
            "NormalizedEmail": email,
            "IsRoleAddress": str(role_addr),
            "TypoFixed": str(typo_fixed),
            "VerificationStatus": "risky",
            "VerificationReason": "soft_role_no_mx_record" if (soft_role and not allow_role) else "no_mx_record",
            "HasMX": "False",
            "HasAorAAAA": str(has_a_aaaa),
            "CheckedAtUTC": checked_at,
        }

    if do_smtp_probe:
        probe_status, probe_reason = smtp_probe(email, mx_hosts=mx_hosts, timeout=timeout, smtp_from=smtp_from)
        status_map = {
            "deliverable": "deliverable",
            "undeliverable": "undeliverable",
            "unknown": "risky",
        }
        return {
            "NormalizedEmail": email,
            "IsRoleAddress": str(role_addr),
            "TypoFixed": str(typo_fixed),
            "VerificationStatus": "risky" if (soft_role and not allow_role) else status_map.get(probe_status, "risky"),
            "VerificationReason": (
                f"soft_role_{probe_reason}" if (soft_role and not allow_role) else probe_reason
            ),
            "HasMX": "True",
            "HasAorAAAA": str(has_a_aaaa),
            "CheckedAtUTC": checked_at,
        }

    return {
        "NormalizedEmail": email,
        "IsRoleAddress": str(role_addr),
        "TypoFixed": str(typo_fixed),
        "VerificationStatus": "risky",
        "VerificationReason": (
            "soft_role_mx_found_no_smtp_probe" if (soft_role and not allow_role) else "mx_found_no_smtp_probe"
        ),
        "HasMX": "True",
        "HasAorAAAA": str(has_a_aaaa),
        "CheckedAtUTC": checked_at,
    }


def read_rows(path: str, max_rows: int) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
            if max_rows > 0 and len(rows) >= max_rows:
                break
    return rows


def write_rows(path: str, rows: List[Dict[str, str]], base_fields: List[str]) -> None:
    out_fields = base_fields + [col for col in RESULT_COLUMNS if col not in base_fields]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    rows = read_rows(args.input, max_rows=max(0, args.max_rows))
    if not rows:
        print(f"[WARN] no rows found in {args.input}")
        return 0

    base_fields = list(rows[0].keys())
    domain_cache: Dict[str, Tuple[bool, bool, List[str]]] = {}

    checked: List[Dict[str, str]] = []
    for idx, row in enumerate(rows, 1):
        verification = verify_email(
            raw_email=row.get(args.email_column, ""),
            timeout=max(1.0, args.timeout),
            do_smtp_probe=args.smtp_probe,
            smtp_from=args.smtp_from,
            allow_role=args.allow_role,
            domain_cache=domain_cache,
        )
        row_out = dict(row)
        row_out.update(verification)
        checked.append(row_out)

        if idx % 25 == 0:
            print(f"[INFO] verified {idx}/{len(rows)} rows")
        time.sleep(max(0.0, args.sleep))

    write_rows(args.output, checked, base_fields=base_fields)

    status_counts: Dict[str, int] = {}
    for row in checked:
        status = row.get("VerificationStatus", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    status_summary = ", ".join(f"{k}={v}" for k, v in sorted(status_counts.items()))
    print(f"[OK] wrote {len(checked)} rows to {args.output}")
    print(f"[INFO] status counts: {status_summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
