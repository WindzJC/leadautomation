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
    assert out["EmailType"] == "generic"
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
    assert out["EmailType"] == "role"


def test_visible_obfuscated_email_is_normalized() -> None:
    out = verify_email(
        raw_email="Reach Jane at jane (at) example (dot) com",
        timeout=2.0,
        do_smtp_probe=False,
        smtp_from="verify@localhost",
        allow_role=False,
        domain_cache={"example.com": (True, True, ["mx.example.com"])},
    )
    assert out["NormalizedEmail"] == "jane@example.com"
    assert out["VerificationStatus"] == "risky"
    assert out["EmailType"] == "personal"


def test_visible_obfuscated_email_with_multi_part_tld_is_normalized() -> None:
    out = verify_email(
        raw_email="Reach Jane at jane [at] books-example [dot] co [dot] uk",
        timeout=2.0,
        do_smtp_probe=False,
        smtp_from="verify@localhost",
        allow_role=False,
        domain_cache={"books-example.co.uk": (True, True, ["mx.example.com"])},
    )
    assert out["NormalizedEmail"] == "jane@books-example.co.uk"
    assert out["VerificationStatus"] == "risky"
    assert out["EmailType"] == "personal"


def test_soft_role_prefix_email_is_classified_as_generic() -> None:
    out = verify_email(
        raw_email="support.team@example.com",
        timeout=2.0,
        do_smtp_probe=False,
        smtp_from="verify@localhost",
        allow_role=False,
        domain_cache={"example.com": (True, True, ["mx.example.com"])},
    )
    assert out["VerificationStatus"] == "risky"
    assert out["IsRoleAddress"] == "True"
    assert out["EmailType"] == "generic"


def test_hidden_or_script_email_source_is_rejected() -> None:
    out = verify_email(
        raw_email='<script>var x=\"jane@example.com\"</script>',
        timeout=2.0,
        do_smtp_probe=False,
        smtp_from="verify@localhost",
        allow_role=False,
        domain_cache={},
    )
    assert out["VerificationStatus"] == "invalid"
    assert out["VerificationReason"] == "email_not_visible_text"
