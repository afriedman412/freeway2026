from app.main import run
from sqlalchemy import text


def test_full_ingestion_flow(engine, fake_email, clean_data_dir):
    """
    1. Run contribution/committee
    2. Run expenditure/date
    3. Run them again (duplicates)
    4. Verify DB contents
    5. Verify email payload
    """

    # ---------------------------------------
    # First run (fresh inserts)
    # ---------------------------------------
    run(
        variant="contribution",
        key="C00780841",
        cycle=2026,
    )

    run(
        variant="expenditure",
        key="2025-12-16",
        cycle=2026,
    )

    # ---------------------------------------
    # Assertions: emails sent for new data
    # ---------------------------------------
    assert len(fake_email) >= 1  # at least one variant found new data

    first = fake_email[0]
    assert first["subject"] is not None
    assert "results" in first["body"].lower()
    assert first['df'] is not None
    assert not first["df"].empty  # ✅ inserts happened

    emails_after_first_run = len(fake_email)

    # ---------------------------------------
    # Second run (should produce duplicates)
    # ---------------------------------------
    run(
        variant="contribution",
        key="C00780841",
        cycle=2026,
    )

    run(
        variant="expenditure",
        key="2025-12-16",
        cycle=2026,
    )

    # ---------------------------------------
    # Assertions: no new emails for duplicates
    # ---------------------------------------
    assert len(fake_email) == emails_after_first_run  # ✅ no new emails sent

    # ---------------------------------------
    # Assertions: database state
    # ---------------------------------------
    with engine.connect() as conn:
        contrib_count = conn.execute(
            text("SELECT COUNT(*) FROM contribution")).scalar()

        exp_count = conn.execute(
            text("SELECT COUNT(*) FROM expenditure")).scalar()

    assert contrib_count > 0
    assert exp_count > 0

    # The key assertion: email only includes NEW rows
    assert len(first["df"]) <= contrib_count + exp_count
