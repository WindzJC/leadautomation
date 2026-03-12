from __future__ import annotations

from verify_emails import verify_email


def test_soft_role_address_is_risky_not_blocked() -> None:
    out = verify_email(
        raw_email="contact@example.com",
        timeout=2.0,
        do_smtp_probe=False,
        smtp_from="verify@localhost",
        allow_role=False,
        domain_cache={"example.com": (True, True, ["mx.example.com"])},
    )
    assert out["VerificationStatus"] == "risky"
    assert out["IsRoleAddress"] == "True"
    assert out["VerificationReason"].startswith("soft_role_")


def test_hard_role_address_is_blocked() -> None:
    out = verify_email(
        raw_email="abuse@example.com",
        timeout=2.0,
        do_smtp_probe=False,
        smtp_from="verify@localhost",
        allow_role=False,
        domain_cache={"example.com": (True, True, ["mx.example.com"])},
    )
    assert out["VerificationStatus"] == "blocked_role"
    assert out["IsRoleAddress"] == "True"
