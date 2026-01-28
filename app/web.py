import os
import secrets
from fastapi import FastAPI, Depends, HTTPException, status, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from app.db import (
    get_engine, create_tables, get_large_expenditures,
    get_config, set_config, get_latest_expenditure
)
from app.mail import send_email
from app.config import TARGET_EMAILS
from app.helpers import get_now

app = FastAPI(title="Sludgewire")
security = HTTPBasic()

ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "changeme")


def verify_admin(credentials: HTTPBasicCredentials = Depends(security)):
    correct_user = secrets.compare_digest(credentials.username, ADMIN_USER)
    correct_pass = secrets.compare_digest(credentials.password, ADMIN_PASS)
    if not (correct_user and correct_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


@app.on_event("startup")
def startup():
    create_tables()


@app.get("/debug")
def debug_db():
    from sqlmodel import Session, select, func
    from app.schemas import Expenditure, Committee
    engine = get_engine()
    with Session(engine) as session:
        exp_count = session.exec(select(func.count()).select_from(Expenditure)).one()
        comm_count = session.exec(select(func.count()).select_from(Committee)).one()

        # Get top 10 expenditures by amount
        top_exp = session.exec(
            select(Expenditure.expenditure_amount, Expenditure.expenditure_date, Expenditure.payee_name)
            .order_by(Expenditure.expenditure_amount.desc())
            .limit(10)
        ).all()

        return {
            "expenditures": exp_count,
            "committees": comm_count,
            "top_10_by_amount": [
                {"amount": r[0], "date": r[1], "payee": r[2]} for r in top_exp
            ]
        }


@app.get("/", response_class=HTMLResponse)
def landing_page():
    """Show unique committees with large filings, linking to FEC pages."""
    engine = get_engine()
    df = get_large_expenditures(engine, min_amount=50000)

    if df.empty:
        table_html = "<p>No committees with filings >= $50k found.</p>"
    else:
        # Get unique committees with their total and max filing
        committees = df.groupby(["committee_id", "committee_name"]).agg({
            "expenditure_amount": ["sum", "max", "count"]
        }).reset_index()
        committees.columns = ["committee_id", "committee_name", "total", "largest", "num_filings"]
        committees = committees.sort_values("total", ascending=False)

        # Build table with FEC links
        rows = []
        for _, row in committees.iterrows():
            fec_link = f'<a href="https://www.fec.gov/data/committee/{row["committee_id"]}/" target="_blank">{row["committee_id"]}</a>'
            rows.append({
                "Committee": row["committee_name"] or "Unknown",
                "FEC ID": fec_link,
                "Total": f"${row['total']:,.2f}",
                "Largest": f"${row['largest']:,.2f}",
                "# Filings": int(row["num_filings"])
            })

        import pandas as pd
        display_df = pd.DataFrame(rows)
        table_html = display_df.to_html(index=False, classes="table", escape=False)

    return f"""
    <html>
    <head><title>Sludgewire - Large Filers</title></head>
    <body>
        <h1>Committees with Filings >= $50k</h1>
        {table_html}
        <p><a href="/transactions">View all transactions</a> | <a href="/config">Config</a></p>
    </body>
    </html>
    """


@app.get("/transactions", response_class=HTMLResponse)
def transactions_page():
    """Show individual transactions >= $50k."""
    engine = get_engine()
    df = get_large_expenditures(engine, min_amount=50000)

    if df.empty:
        table_html = "<p>No expenditures >= $50k found.</p>"
    else:
        # Select key columns for display
        display_cols = [
            "transaction_id", "expenditure_date", "expenditure_amount",
            "committee_name", "payee_name", "expenditure_description"
        ]
        display_df = df[[c for c in display_cols if c in df.columns]].copy()
        display_df["expenditure_amount"] = display_df["expenditure_amount"].apply(
            lambda x: f"${x:,.2f}" if x else ""
        )
        table_html = display_df.to_html(index=False, classes="table", escape=False)

    return f"""
    <html>
    <head><title>Sludgewire - Transactions</title></head>
    <body>
        <h1>Expenditures >= $50k</h1>
        {table_html}
        <p><a href="/">Back to committees</a> | <a href="/config">Config</a></p>
    </body>
    </html>
    """


@app.get("/config", response_class=HTMLResponse)
def config_page(user: str = Depends(verify_admin)):
    engine = get_engine()

    target_emails = get_config(engine, "target_emails", TARGET_EMAILS)
    target_pacs = get_config(engine, "target_pacs", [])

    return f"""
    <html>
    <head><title>Sludgewire - Config</title></head>
    <body>
        <h1>Config</h1>
        <form method="post" action="/config">
            <h3>Target Emails (one per line)</h3>
            <textarea name="emails" rows="5" cols="40">{chr(10).join(target_emails)}</textarea>

            <h3>Target PAC IDs (one per line)</h3>
            <textarea name="pacs" rows="5" cols="40">{chr(10).join(target_pacs)}</textarea>

            <br><br>
            <button type="submit">Save</button>
        </form>
        <hr>
        <h3>Send Test Email</h3>
        <form method="post" action="/send-test">
            <button type="submit">Send Test Email</button>
        </form>
        <p><a href="/">Back to filings</a></p>
    </body>
    </html>
    """


@app.post("/config", response_class=HTMLResponse)
def save_config(
    emails: str = Form(...),
    pacs: str = Form(...),
    user: str = Depends(verify_admin)
):
    engine = get_engine()

    email_list = [e.strip() for e in emails.strip().split("\n") if e.strip()]
    pac_list = [p.strip() for p in pacs.strip().split("\n") if p.strip()]

    set_config(engine, "target_emails", email_list)
    set_config(engine, "target_pacs", pac_list)

    return """
    <html>
    <head><title>Config Saved</title></head>
    <body>
        <h1>Config saved!</h1>
        <p><a href="/config">Back to config</a></p>
    </body>
    </html>
    """


@app.post("/send-test", response_class=HTMLResponse)
def send_test(user: str = Depends(verify_admin)):
    engine = get_engine()

    target_emails = get_config(engine, "target_emails", TARGET_EMAILS)
    latest_df = get_latest_expenditure(engine)

    if latest_df.empty:
        return """
        <html><body>
            <h1>No expenditures in database</h1>
            <p><a href="/config">Back</a></p>
        </body></html>
        """

    ts = get_now()
    row = latest_df.iloc[0]
    recipients_list = "\n".join(f"  - {email}" for email in target_emails)

    body = f"""[TEST MODE] Most recent expenditure as of {ts}

Recipients:
{recipients_list}

Latest Record:
--------------
Date:          {row['expenditure_date']}
Amount:        ${row['expenditure_amount']:,.2f}
Committee:     {row['committee_name']} ({row['committee_id']})
Payee:         {row['payee_name']}
Description:   {row['expenditure_description']}
"""

    send_email(
        subject=f"[sludgewire] TEST - Last Update Check, {ts}",
        body=body,
        to=target_emails,
        sender="steadynappin@gmail.com",
        df=latest_df,
        attachment_format="json"
    )

    return f"""
    <html><body>
        <h1>Test email sent!</h1>
        <p>Sent to: {', '.join(target_emails)}</p>
        <p><a href="/config">Back</a></p>
    </body></html>
    """
