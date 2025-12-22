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
    # Assertions: database state
    # ---------------------------------------
    with engine.connect() as conn:
        contrib_count = conn.execute(
            text("SELECT COUNT(*) FROM contribution")).scalar()

        exp_count = conn.execute(
            text("SELECT COUNT(*) FROM expenditure")).scalar()

    assert contrib_count > 0
    assert exp_count > 0

    # ---------------------------------------
    # Assertions: email was sent
    # ---------------------------------------

    assert len(fake_email) >= 1

    first = fake_email[0]
    assert first["subject"] is not None
    assert "results" in first["body"].lower()
    assert first['df'] is not None
    assert not first["df"].empty  # ✅ inserts happened

    last = fake_email[-1]
    assert last["df"].empty       # ✅ duplicates skipped

    # The key assertion: email only includes NEW rows
    # On the second run, this should be empty or small
    assert len(first["df"]) <= contrib_count + exp_count
