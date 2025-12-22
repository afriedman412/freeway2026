from typing import Optional
from sqlmodel import SQLModel, Field
from sqlalchemy import Column, Text, Numeric, BigInteger, PrimaryKeyConstraint


# ============================================================
# NESTED OBJECTS
# ============================================================

class Committee(SQLModel, table=True):
    __tablename__ = "committee"

    committee_id: str = Field(primary_key=True)

    affiliated_committee_name: Optional[str] = None
    candidate_ids: Optional[str] = None

    name: Optional[str] = None
    party: Optional[str] = None
    party_full: Optional[str] = None

    designation: Optional[str] = None
    designation_full: Optional[str] = None

    committee_type: Optional[str] = None
    committee_type_full: Optional[str] = None
    organization_type: Optional[str] = None
    organization_type_full: Optional[str] = None

    city: Optional[str] = None
    state: Optional[str] = None
    state_full: Optional[str] = None
    zip: Optional[str] = None
    street_1: Optional[str] = None
    street_2: Optional[str] = None

    treasurer_name: Optional[str] = None
    filing_frequency: Optional[str] = None

    is_active: Optional[bool] = None

    # cycle-related fields are identifiers, not math → STRING
    cycle: Optional[str] = None
    cycles: Optional[str] = None
    cycles_has_activity: Optional[str] = None
    cycles_has_financial: Optional[str] = None
    last_cycle_has_activity: Optional[str] = None
    last_cycle_has_financial: Optional[str] = None

    first_f1_date: Optional[str] = None
    last_f1_date: Optional[str] = None
    first_file_date: Optional[str] = None
    last_file_date: Optional[str] = None


# ============================================================
# SCHEDULE A — CONTRIBUTIONS
# ============================================================

class Contribution(SQLModel, table=True):
    __tablename__ = "contribution"
    __table_args__ = (
        PrimaryKeyConstraint("sub_id", "transaction_id"),
    )
    sub_id: str = Field(nullable=False)
    transaction_id: str = Field(nullable=False)

    committee_id: Optional[str] = Field(index=True)

    amendment_indicator: Optional[str] = Field(sa_column=Column(Text))
    amendment_indicator_desc: Optional[str] = Field(sa_column=Column(Text))

    contribution_receipt_amount: Optional[float] = Field(
        sa_column=Column(Numeric))
    contribution_receipt_date: Optional[str] = Field(sa_column=Column(Text))

    contributor_name: Optional[str] = Field(sa_column=Column(Text))
    contributor_first_name: Optional[str] = Field(sa_column=Column(Text))
    contributor_middle_name: Optional[str] = Field(sa_column=Column(Text))
    contributor_last_name: Optional[str] = Field(sa_column=Column(Text))

    contributor_city: Optional[str] = Field(sa_column=Column(Text))
    contributor_state: Optional[str] = Field(sa_column=Column(Text))
    contributor_zip: Optional[str] = Field(sa_column=Column(Text))
    contributor_street_1: Optional[str] = Field(sa_column=Column(Text))
    contributor_street_2: Optional[str] = Field(sa_column=Column(Text))

    contributor_employer: Optional[str] = Field(sa_column=Column(Text))
    contributor_occupation: Optional[str] = Field(sa_column=Column(Text))
    contributor_aggregate_ytd: Optional[float] = Field(
        sa_column=Column(Numeric))

    entity_type: Optional[str] = Field(sa_column=Column(Text))
    entity_type_desc: Optional[str] = Field(sa_column=Column(Text))

    election_type: Optional[str] = Field(sa_column=Column(Text))
    election_type_full: Optional[str] = Field(sa_column=Column(Text))
    fec_election_type_desc: Optional[str] = Field(sa_column=Column(Text))

    filing_form: Optional[str] = Field(sa_column=Column(Text))
    file_number: Optional[int] = Field(sa_column=Column(BigInteger))
    image_number: Optional[str] = Field(sa_column=Column(Text))
    pdf_url: Optional[str] = Field(sa_column=Column(Text))

    report_type: Optional[str] = Field(sa_column=Column(Text))
    report_year: Optional[str] = Field(sa_column=Column(Text))
    schedule_type: Optional[str] = Field(sa_column=Column(Text))
    schedule_type_full: Optional[str] = Field(sa_column=Column(Text))

    link_id: Optional[int] = Field(sa_column=Column(BigInteger))
    two_year_transaction_period: Optional[str] = Field(sa_column=Column(Text))


# ============================================================
# SCHEDULE E — EXPENDITURES
# ============================================================

class Expenditure(SQLModel, table=True):
    __tablename__ = "expenditure"
    __table_args__ = (
        PrimaryKeyConstraint("sub_id", "transaction_id"),
    )

    # 🔑 MUST be non-nullable
    sub_id: str = Field(nullable=False)
    transaction_id: str = Field(nullable=False)

    committee_id: Optional[str] = Field(sa_column=Column(Text))
    candidate_id: Optional[str] = Field(sa_column=Column(Text))

    amendment_indicator: Optional[str] = Field(sa_column=Column(Text))
    action_code: Optional[str] = Field(sa_column=Column(Text))
    action_code_full: Optional[str] = Field(sa_column=Column(Text))

    expenditure_amount: Optional[float] = Field(sa_column=Column(Numeric))
    expenditure_date: Optional[str] = Field(sa_column=Column(Text))
    expenditure_description: Optional[str] = Field(sa_column=Column(Text))

    disbursement_dt: Optional[str] = Field(sa_column=Column(Text))
    dissemination_date: Optional[str] = Field(sa_column=Column(Text))

    election_type: Optional[str] = Field(sa_column=Column(Text))
    election_type_full: Optional[str] = Field(sa_column=Column(Text))

    payee_name: Optional[str] = Field(sa_column=Column(Text))
    payee_city: Optional[str] = Field(sa_column=Column(Text))
    payee_state: Optional[str] = Field(sa_column=Column(Text))
    payee_street_1: Optional[str] = Field(sa_column=Column(Text))
    payee_street_2: Optional[str] = Field(sa_column=Column(Text))
    payee_zip: Optional[str] = Field(sa_column=Column(Text))

    support_oppose_indicator: Optional[str] = Field(sa_column=Column(Text))
    office_total_ytd: Optional[float] = Field(sa_column=Column(Numeric))

    filing_form: Optional[str] = Field(sa_column=Column(Text))
    filing_date: Optional[str] = Field(sa_column=Column(Text))
    file_number: Optional[int] = Field(sa_column=Column(BigInteger))
    previous_file_number: Optional[int] = Field(sa_column=Column(BigInteger))
    image_number: Optional[str] = Field(sa_column=Column(Text))
    pdf_url: Optional[str] = Field(sa_column=Column(Text))

    report_type: Optional[str] = Field(sa_column=Column(Text))
    report_year: Optional[str] = Field(sa_column=Column(Text))
    schedule_type: Optional[str] = Field(sa_column=Column(Text))
    schedule_type_full: Optional[str] = Field(sa_column=Column(Text))

    link_id: Optional[int] = Field(sa_column=Column(BigInteger))
