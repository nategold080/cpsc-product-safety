"""Streamlit dashboard for CPSC Product Safety Tracker."""

import os
import sqlite3
from pathlib import Path

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

_DATA_DIR = Path(__file__).parent.parent.parent / "data"
_DEPLOY_DB = _DATA_DIR / "cpsc_product_safety_deploy.db"
_FULL_DB = _DATA_DIR / "cpsc_product_safety.db"

_env_db = os.environ.get("CPSC_DB_PATH")
if _env_db and Path(_env_db).exists():
    DB_PATH = Path(_env_db)
elif _DEPLOY_DB.exists():
    DB_PATH = _DEPLOY_DB
elif _FULL_DB.exists():
    DB_PATH = _FULL_DB
else:
    _db_files = list(_DATA_DIR.glob("*.db")) if _DATA_DIR.exists() else []
    DB_PATH = _db_files[0] if _db_files else _FULL_DB


def _table_exists(conn, name):
    try:
        r = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        return r is not None
    except Exception:
        return False


def _safe_query(sql, conn, params=()):
    try:
        return pd.read_sql_query(sql, conn, params=params if params else None)
    except Exception:
        return pd.DataFrame()


def _safe_fetchone(conn, sql, params=(), default=0):
    try:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else default
    except Exception:
        return default


@st.cache_resource
def get_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    return conn


@st.cache_data(ttl=3600)
def run_query(query, params=None):
    conn = get_db()
    try:
        return pd.read_sql_query(query, conn, params=params or [])
    except Exception:
        return pd.DataFrame()


def main():
    st.set_page_config(page_title="CPSC Product Safety Tracker", page_icon="🛡️", layout="wide")

    st.title("CPSC Product Safety Tracker")
    st.caption("Cross-linked database of consumer product recalls, incidents, injuries, penalties, and import violations")

    tabs = st.tabs([
        "Overview", "Product Injury Analysis", "Cross-Agency Analysis",
        "Hazard Validation", "Manufacturer Search", "Top Manufacturers",
        "Risk Analysis", "Recalls", "NEISS Injuries", "Import Violations",
        "Data Explorer"
    ])

    with tabs[0]:
        render_overview()
    with tabs[1]:
        render_product_injury_analysis()
    with tabs[2]:
        render_cross_agency()
    with tabs[3]:
        render_hazard_validation()
    with tabs[4]:
        render_search()
    with tabs[5]:
        render_top_manufacturers()
    with tabs[6]:
        render_risk_analysis()
    with tabs[7]:
        render_recalls()
    with tabs[8]:
        render_neiss()
    with tabs[9]:
        render_violations()
    with tabs[10]:
        render_data_explorer()

    st.markdown("---")
    st.markdown(
        "Built by **Nathan Goldberg** · "
        "[nathanmauricegoldberg@gmail.com](mailto:nathanmauricegoldberg@gmail.com) · "
        "[LinkedIn](https://www.linkedin.com/in/nathan-goldberg-62a44522a/)"
    )


def render_overview():
    st.header("Overview")

    conn = get_db()
    required = ["recalls", "incidents", "neiss_injuries", "penalties", "import_violations",
                "manufacturer_profiles", "cross_links"]
    if not all(_table_exists(conn, t) for t in required):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    # KPI cards
    stats = run_query("""
        SELECT
            (SELECT COUNT(*) FROM recalls) as recalls,
            (SELECT COUNT(*) FROM incidents) as incidents,
            (SELECT COUNT(*) FROM neiss_injuries) as neiss,
            (SELECT COUNT(*) FROM penalties) as penalties,
            (SELECT COUNT(*) FROM import_violations) as violations,
            (SELECT COUNT(*) FROM manufacturer_profiles) as profiles,
            (SELECT COUNT(*) FROM cross_links) as links,
            (SELECT COALESCE(SUM(fine_amount), 0) FROM penalties) as total_fines,
            (SELECT COALESCE(SUM(weight), 0) FROM neiss_injuries) as neiss_estimate,
            (SELECT COUNT(*) FROM manufacturer_profiles WHERE risk_tier IN ('HIGH','CRITICAL')) as high_risk
    """)

    if stats.empty:
        st.warning("No data available.")
        return

    row = stats.iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Recall Campaigns", f"{row['recalls']:,}")
    c2.metric("Incident Reports", f"{row['incidents']:,}")
    c3.metric("NEISS ER Injuries", f"{row['neiss']:,}")
    c4.metric("Penalties", f"{row['penalties']:,}")
    c5.metric("Import Violations", f"{row['violations']:,}")

    c6, c7, c8, c9, c10 = st.columns(5)
    c6.metric("Manufacturer Profiles", f"{row['profiles']:,}")
    c7.metric("Cross-Links", f"{row['links']:,}")
    c8.metric("Total Fines", f"${row['total_fines']:,.0f}")
    c9.metric("Est. National Injuries", f"{row['neiss_estimate']:,.0f}")
    c10.metric("High/Critical Risk", f"{row['high_risk']:,}")

    # Recalls by year
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Recalls by Year")
        df = run_query("SELECT fiscal_year, COUNT(*) as count FROM recalls WHERE fiscal_year IS NOT NULL GROUP BY fiscal_year ORDER BY fiscal_year")
        if not df.empty:
            fig = px.bar(df, x="fiscal_year", y="count", labels={"fiscal_year": "Fiscal Year", "count": "Recalls"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Incidents by Category")
        df = run_query("SELECT product_category, COUNT(*) as count FROM incidents WHERE product_category != '' GROUP BY product_category ORDER BY count DESC LIMIT 10")
        if not df.empty:
            fig = px.bar(df, x="count", y="product_category", orientation="h",
                        labels={"product_category": "Category", "count": "Incidents"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    # Risk tier distribution
    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Risk Tier Distribution")
        df = run_query("SELECT risk_tier, COUNT(*) as count FROM manufacturer_profiles GROUP BY risk_tier ORDER BY count DESC")
        if not df.empty:
            colors = {"LOW": "#2ecc71", "MEDIUM": "#f39c12", "HIGH": "#e74c3c", "CRITICAL": "#8e44ad"}
            fig = px.pie(df, values="count", names="risk_tier",
                        color="risk_tier", color_discrete_map=colors)
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("Top Penalty Amounts")
        df = run_query("""
            SELECT firm_name, SUM(fine_amount) as total_fines, COUNT(*) as count
            FROM penalties WHERE fine_amount > 0
            GROUP BY normalized_firm ORDER BY total_fines DESC LIMIT 10
        """)
        if not df.empty:
            fig = px.bar(df, x="total_fines", y="firm_name", orientation="h",
                        labels={"firm_name": "Firm", "total_fines": "Total Fines ($)"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)


def render_product_injury_analysis():
    st.header("Product Injury Analysis")
    st.caption("NEISS injury data resolved by product category — 2M+ emergency room records")

    conn = get_db()
    if not _table_exists(conn, "neiss_injuries") or not _table_exists(conn, "neiss_product_codes"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    # KPI row
    kpi = run_query("""
        SELECT
            (SELECT COUNT(DISTINCT product_1_name) FROM neiss_injuries
             WHERE product_1_name IS NOT NULL AND product_1_name != '') as products,
            (SELECT COUNT(*) FROM neiss_injuries
             WHERE product_1_name IS NOT NULL AND product_1_name != '') as resolved,
            (SELECT COUNT(*) FROM neiss_injuries) as total,
            (SELECT COUNT(DISTINCT category) FROM neiss_product_codes
             WHERE category != '' AND category IS NOT NULL) as categories,
            (SELECT SUM(weight) FROM neiss_injuries
             WHERE product_1_name IS NOT NULL AND product_1_name != '') as est_injuries
    """)
    if not kpi.empty:
        row = kpi.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Product Types Tracked", f"{row['products']:,}")
        c2.metric("Records Resolved", f"{row['resolved']:,} / {row['total']:,}")
        c3.metric("Product Categories", f"{row['categories']:,}")
        c4.metric("Est. National Injuries", f"{row['est_injuries']:,.0f}")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 20 Products by Injury Count")
        df = run_query("""
            SELECT product_1_name as product, COUNT(*) as injuries,
                   CAST(SUM(weight) AS INTEGER) as national_estimate
            FROM neiss_injuries
            WHERE product_1_name IS NOT NULL AND product_1_name != ''
            GROUP BY product_1_name ORDER BY injuries DESC LIMIT 20
        """)
        if not df.empty:
            fig = px.bar(df, x="injuries", y="product", orientation="h",
                        hover_data=["national_estimate"],
                        labels={"product": "Product", "injuries": "ER Cases",
                                "national_estimate": "Nat'l Estimate"})
            fig.update_layout(template="plotly_dark", height=600)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top 20 Products by National Estimate")
        df = run_query("""
            SELECT product_1_name as product,
                   CAST(SUM(weight) AS INTEGER) as national_estimate,
                   COUNT(*) as injuries
            FROM neiss_injuries
            WHERE product_1_name IS NOT NULL AND product_1_name != ''
            GROUP BY product_1_name ORDER BY national_estimate DESC LIMIT 20
        """)
        if not df.empty:
            fig = px.bar(df, x="national_estimate", y="product", orientation="h",
                        hover_data=["injuries"],
                        labels={"product": "Product", "national_estimate": "Nat'l Estimate",
                                "injuries": "ER Cases"})
            fig.update_layout(template="plotly_dark", height=600)
            st.plotly_chart(fig, use_container_width=True)

    # Category breakdown
    st.subheader("Injuries by Product Category")
    df = run_query("""
        SELECT pc.category, COUNT(*) as injuries,
               CAST(SUM(ni.weight) AS INTEGER) as national_estimate
        FROM neiss_injuries ni
        JOIN neiss_product_codes pc ON ni.product_1 = pc.product_code
        WHERE pc.category IS NOT NULL AND pc.category != ''
        GROUP BY pc.category ORDER BY injuries DESC
    """)
    if not df.empty:
        fig = px.bar(df, x="injuries", y="category", orientation="h",
                    hover_data=["national_estimate"],
                    labels={"category": "Category", "injuries": "ER Cases"})
        fig.update_layout(template="plotly_dark", height=500)
        st.plotly_chart(fig, use_container_width=True)

    # Product × Diagnosis heatmap
    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Product × Diagnosis (Top 10 each)")
        df = run_query("""
            SELECT product_1_name as product, diagnosis_name as diagnosis,
                   COUNT(*) as count
            FROM neiss_injuries
            WHERE product_1_name IN (
                SELECT product_1_name FROM neiss_injuries
                WHERE product_1_name IS NOT NULL AND product_1_name != ''
                GROUP BY product_1_name ORDER BY COUNT(*) DESC LIMIT 10
            )
            AND diagnosis_name IS NOT NULL AND diagnosis_name != ''
            GROUP BY product_1_name, diagnosis_name
        """)
        if not df.empty:
            pivot = df.pivot_table(index="product", columns="diagnosis",
                                   values="count", fill_value=0)
            # Keep top 10 diagnoses by total
            top_diag = df.groupby("diagnosis")["count"].sum().nlargest(10).index
            pivot = pivot[[c for c in top_diag if c in pivot.columns]]
            fig = px.imshow(pivot, aspect="auto",
                          labels={"x": "Diagnosis", "y": "Product", "color": "Cases"})
            fig.update_layout(template="plotly_dark", height=500)
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("Child-Related Product Injuries")
        df = run_query("""
            SELECT ni.product_1_name as product, COUNT(*) as injuries,
                   CAST(SUM(ni.weight) AS INTEGER) as national_estimate
            FROM neiss_injuries ni
            JOIN neiss_product_codes pc ON ni.product_1 = pc.product_code
            WHERE pc.is_child_related = 1
            GROUP BY ni.product_1_name ORDER BY injuries DESC LIMIT 15
        """)
        if not df.empty:
            fig = px.bar(df, x="injuries", y="product", orientation="h",
                        labels={"product": "Product", "injuries": "ER Cases"})
            fig.update_layout(template="plotly_dark", height=500)
            st.plotly_chart(fig, use_container_width=True)

    # Yearly trends for top products
    st.subheader("Yearly Injury Trends — Top 5 Products")
    df = run_query("""
        SELECT neiss_year, product_1_name as product,
               COUNT(*) as injuries
        FROM neiss_injuries
        WHERE product_1_name IN (
            SELECT product_1_name FROM neiss_injuries
            WHERE product_1_name IS NOT NULL AND product_1_name != ''
            GROUP BY product_1_name ORDER BY COUNT(*) DESC LIMIT 5
        )
        AND neiss_year IS NOT NULL
        GROUP BY neiss_year, product_1_name
        ORDER BY neiss_year
    """)
    if not df.empty:
        fig = px.line(df, x="neiss_year", y="injuries", color="product",
                     labels={"neiss_year": "Year", "injuries": "ER Cases", "product": "Product"})
        fig.update_layout(template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)


def render_hazard_validation():
    st.header("Hazard Validation")
    st.caption("Do NEISS injury diagnoses match the hazard types stated in recalls?")

    conn = get_db()
    if not _table_exists(conn, "hazard_validation_results"):
        st.info("No hazard validation data. Run `python3 -m src.cli crosslink` to generate.")
        return

    # KPI row
    kpi = run_query("""
        SELECT
            (SELECT COUNT(*) FROM hazard_validation_results) as total,
            (SELECT COUNT(*) FROM hazard_validation_results WHERE validation_status = 'confirmed') as confirmed,
            (SELECT COUNT(*) FROM hazard_validation_results WHERE validation_status = 'unexpected_pattern') as unexpected,
            (SELECT COUNT(*) FROM hazard_validation_results WHERE validation_status = 'insufficient_data') as insufficient,
            (SELECT COUNT(DISTINCT manufacturer_normalized) FROM hazard_validation_results) as manufacturers
    """)
    if not kpi.empty:
        row = kpi.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Validations", f"{row['total']:,}")
        c2.metric("Confirmed", f"{row['confirmed']:,}")
        c3.metric("Unexpected Pattern", f"{row['unexpected']:,}")
        c4.metric("Insufficient Data", f"{row['insufficient']:,}")
        c5.metric("Manufacturers Validated", f"{row['manufacturers']:,}")

    if kpi.empty or kpi.iloc[0]["total"] == 0:
        st.info("No hazard validation data. Run `python3 -m src.cli crosslink` to generate.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Validation by Hazard Type")
        df = run_query("""
            SELECT hazard_type, validation_status, COUNT(*) as count
            FROM hazard_validation_results
            GROUP BY hazard_type, validation_status
            ORDER BY hazard_type
        """)
        if not df.empty:
            fig = px.bar(df, x="hazard_type", y="count", color="validation_status",
                        color_discrete_map={"confirmed": "#2ecc71",
                                            "unexpected_pattern": "#e74c3c",
                                            "insufficient_data": "#f39c12"},
                        labels={"hazard_type": "Hazard Type", "count": "Manufacturers"})
            fig.update_layout(template="plotly_dark", xaxis_tickangle=45)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Average Match Rate by Hazard Type")
        df = run_query("""
            SELECT hazard_type, AVG(match_rate) as avg_rate, COUNT(*) as count
            FROM hazard_validation_results
            WHERE total_neiss_injuries >= 10
            GROUP BY hazard_type ORDER BY avg_rate DESC
        """)
        if not df.empty:
            fig = px.bar(df, x="avg_rate", y="hazard_type", orientation="h",
                        labels={"hazard_type": "Hazard Type", "avg_rate": "Average Match Rate"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    # Unexpected pattern manufacturers
    st.subheader("Manufacturers with Unexpected Injury Patterns")
    st.caption("Hazard type in recall does not match common NEISS injury diagnoses")
    df = run_query("""
        SELECT manufacturer_normalized as manufacturer, hazard_type,
               total_recalls_with_hazard as recalls, total_neiss_injuries as injuries,
               matching_diagnosis_count as matching, unexpected_diagnosis_count as unexpected,
               ROUND(match_rate, 3) as match_rate
        FROM hazard_validation_results
        WHERE validation_status = 'unexpected_pattern'
        ORDER BY total_neiss_injuries DESC
        LIMIT 50
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=400)

    # Hazard-diagnosis heatmap
    st.subheader("Hazard-Diagnosis Reference Map")
    df = run_query("""
        SELECT hazard_type, diagnosis_name
        FROM hazard_diagnosis_map
        WHERE diagnosis_name IS NOT NULL AND diagnosis_name != ''
        ORDER BY hazard_type, diagnosis_name
    """)
    if not df.empty:
        df["match"] = 1
        pivot = df.pivot_table(index="hazard_type", columns="diagnosis_name",
                               values="match", fill_value=0)
        fig = px.imshow(pivot, aspect="auto",
                       labels={"x": "Expected Diagnosis", "y": "Hazard Type", "color": "Expected Match"})
        fig.update_layout(template="plotly_dark", height=500)
        st.plotly_chart(fig, use_container_width=True)


def render_cross_agency():
    st.header("Cross-Agency Analysis")
    st.caption("Manufacturers appearing in BOTH CPSC and FDA enforcement systems")

    conn = get_db()
    fda_tables = ["fda_adverse_events", "fda_device_recalls", "cpsc_fda_manufacturer_links"]
    if not all(_table_exists(conn, t) for t in fda_tables):
        st.info("No FDA data loaded yet. Run `python3 -m src.cli scrape-fda` to import FDA data.")
        return

    # KPI row
    kpi = run_query("""
        SELECT
            (SELECT COUNT(*) FROM fda_adverse_events) as fda_events,
            (SELECT COUNT(*) FROM fda_device_recalls) as fda_recalls,
            (SELECT COUNT(*) FROM cpsc_fda_manufacturer_links) as linked_mfrs,
            (SELECT COUNT(*) FROM cross_links WHERE link_type = 'cross_agency_manufacturer') as cross_links
    """)
    if not kpi.empty:
        row = kpi.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("FDA Adverse Events", f"{row['fda_events']:,}")
        c2.metric("FDA Device Recalls", f"{row['fda_recalls']:,}")
        c3.metric("Cross-Agency Manufacturers", f"{row['linked_mfrs']:,}")
        c4.metric("Cross-Agency Links", f"{row['cross_links']:,}")

    if kpi.empty or kpi.iloc[0]["fda_events"] == 0:
        st.info("No FDA data loaded yet. Run `python3 -m src.cli scrape-fda` to import FDA data.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Manufacturers in BOTH CPSC & FDA")
        df = run_query("""
            SELECT l.cpsc_manufacturer as manufacturer,
                   mp.total_recalls as cpsc_recalls,
                   mp.total_incidents as cpsc_incidents,
                   (SELECT COUNT(*) FROM fda_adverse_events
                    WHERE manufacturer_normalized = l.fda_manufacturer) as fda_events,
                   (SELECT COUNT(*) FROM fda_device_recalls
                    WHERE manufacturer_normalized = l.fda_manufacturer) as fda_recalls,
                   mp.compliance_score, mp.risk_tier
            FROM cpsc_fda_manufacturer_links l
            JOIN manufacturer_profiles mp ON mp.normalized_name = l.cpsc_manufacturer
            ORDER BY (mp.total_recalls + fda_events) DESC
            LIMIT 30
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True, height=400)

    with col2:
        st.subheader("FDA Event Types")
        df = run_query("""
            SELECT event_type, COUNT(*) as count
            FROM fda_adverse_events
            WHERE event_type IS NOT NULL AND event_type != ''
            GROUP BY event_type ORDER BY count DESC
        """)
        if not df.empty:
            fig = px.pie(df, values="count", names="event_type")
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Top FDA Manufacturers by Adverse Events")
        df = run_query("""
            SELECT manufacturer_name, COUNT(*) as events
            FROM fda_adverse_events
            WHERE manufacturer_name IS NOT NULL AND manufacturer_name != ''
            GROUP BY manufacturer_normalized ORDER BY events DESC LIMIT 15
        """)
        if not df.empty:
            fig = px.bar(df, x="events", y="manufacturer_name", orientation="h",
                        labels={"manufacturer_name": "Manufacturer", "events": "Events"})
            fig.update_layout(template="plotly_dark", height=450)
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("FDA Patient Outcomes")
        df = run_query("""
            SELECT patient_outcome, COUNT(*) as count
            FROM fda_adverse_events
            WHERE patient_outcome IS NOT NULL AND patient_outcome != ''
            GROUP BY patient_outcome ORDER BY count DESC LIMIT 10
        """)
        if not df.empty:
            fig = px.bar(df, x="count", y="patient_outcome", orientation="h",
                        labels={"patient_outcome": "Outcome", "count": "Events"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    # FDA recall class distribution
    st.subheader("FDA Device Recalls by Status")
    df = run_query("""
        SELECT recall_status, COUNT(*) as count
        FROM fda_device_recalls
        WHERE recall_status IS NOT NULL AND recall_status != ''
        GROUP BY recall_status ORDER BY count DESC
    """)
    if not df.empty:
        fig = px.bar(df, x="recall_status", y="count",
                    labels={"recall_status": "Status", "count": "Recalls"})
        fig.update_layout(template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)


def render_search():
    st.header("Manufacturer Search")

    conn = get_db()
    if not _table_exists(conn, "manufacturer_profiles"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    query = st.text_input("Search manufacturer name", placeholder="e.g., Samsung, IKEA, Fisher-Price")

    if query:
        df = run_query("""
            SELECT manufacturer_name, normalized_name,
                   total_recalls, total_units_recalled,
                   total_incidents, total_penalties, total_fines,
                   total_import_violations, compliance_score, risk_tier,
                   first_seen_date, last_seen_date, data_sources
            FROM manufacturer_profiles
            WHERE manufacturer_name LIKE ? OR normalized_name LIKE ?
            ORDER BY total_recalls DESC LIMIT 50
        """, [f"%{query}%", f"%{query.upper()}%"])

        if df.empty:
            st.warning("No manufacturers found matching your search.")
        else:
            st.info(f"Found {len(df)} manufacturer(s)")
            for _, row in df.iterrows():
                with st.expander(f"{row['manufacturer_name']} — {row['risk_tier']} Risk (Score: {row['compliance_score']:.3f})"):
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Recalls", f"{row['total_recalls']:,}")
                    c2.metric("Incidents", f"{row['total_incidents']:,}")
                    c3.metric("Penalties", f"{row['total_penalties']:,}")
                    c4.metric("Fines", f"${row['total_fines']:,.0f}")

                    c5, c6, c7, c8 = st.columns(4)
                    c5.metric("Units Recalled", f"{row['total_units_recalled']:,}")
                    c6.metric("Import Violations", f"{row['total_import_violations']:,}")
                    c7.metric("First Seen", row['first_seen_date'] or "N/A")
                    c8.metric("Last Seen", row['last_seen_date'] or "N/A")

                    st.text(f"Data sources: {row['data_sources']}")


def render_top_manufacturers():
    st.header("Top Manufacturers")

    conn = get_db()
    if not _table_exists(conn, "manufacturer_profiles"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    sort_by = st.selectbox("Sort by", ["Recalls", "Incidents", "Fines", "Import Violations", "Compliance Score"])
    sort_map = {
        "Recalls": "total_recalls DESC",
        "Incidents": "total_incidents DESC",
        "Fines": "total_fines DESC",
        "Import Violations": "total_import_violations DESC",
        "Compliance Score": "compliance_score ASC",
    }

    df = run_query(f"""
        SELECT manufacturer_name, total_recalls, total_incidents,
               total_penalties, total_fines, total_import_violations,
               compliance_score, risk_tier
        FROM manufacturer_profiles
        ORDER BY {sort_map[sort_by]}
        LIMIT 100
    """)

    if not df.empty:
        st.dataframe(df, use_container_width=True, height=600)

        csv = df.to_csv(index=False)
        st.download_button("Download CSV", csv, "top_manufacturers.csv", "text/csv")


def render_risk_analysis():
    st.header("Risk Analysis")

    conn = get_db()
    if not _table_exists(conn, "manufacturer_profiles"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    # Compliance score distribution
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Compliance Score Distribution")
        df = run_query("SELECT compliance_score FROM manufacturer_profiles")
        if not df.empty:
            fig = px.histogram(df, x="compliance_score", nbins=50,
                             labels={"compliance_score": "Compliance Score"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Recalls vs Incidents")
        df = run_query("""
            SELECT manufacturer_name, total_recalls, total_incidents,
                   compliance_score, risk_tier
            FROM manufacturer_profiles
            WHERE total_recalls > 0 OR total_incidents > 0
            ORDER BY total_recalls DESC LIMIT 200
        """)
        if not df.empty:
            fig = px.scatter(df, x="total_recalls", y="total_incidents",
                           color="risk_tier", hover_name="manufacturer_name",
                           size="total_incidents",
                           color_discrete_map={"LOW": "#2ecc71", "MEDIUM": "#f39c12",
                                             "HIGH": "#e74c3c", "CRITICAL": "#8e44ad"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    # High/Critical risk table
    st.subheader("High & Critical Risk Manufacturers")
    df = run_query("""
        SELECT manufacturer_name, total_recalls, total_incidents,
               total_penalties, total_fines, total_import_violations,
               compliance_score, risk_tier
        FROM manufacturer_profiles
        WHERE risk_tier IN ('HIGH', 'CRITICAL')
        ORDER BY compliance_score ASC
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)


def render_recalls():
    st.header("Recall Campaigns")

    conn = get_db()
    if not _table_exists(conn, "recalls"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Recalls by Hazard Type")
        df = run_query("""
            SELECT hazard_types, COUNT(*) as count FROM recalls
            WHERE hazard_types != '' AND hazard_types IS NOT NULL
            GROUP BY hazard_types ORDER BY count DESC LIMIT 15
        """)
        if not df.empty:
            fig = px.bar(df, x="count", y="hazard_types", orientation="h",
                        labels={"hazard_types": "Hazard Type", "count": "Recalls"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Manufacturing Countries")
        df = run_query("""
            SELECT manufacturer_countries, COUNT(*) as count FROM recalls
            WHERE manufacturer_countries != '' AND manufacturer_countries IS NOT NULL
            GROUP BY manufacturer_countries ORDER BY count DESC LIMIT 10
        """)
        if not df.empty:
            fig = px.pie(df, values="count", names="manufacturer_countries")
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    # Recent recalls
    st.subheader("Recent Recalls")
    df = run_query("""
        SELECT recall_number, recall_date, title, manufacturer_names,
               hazard_description, number_of_units
        FROM recalls ORDER BY recall_date DESC LIMIT 50
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=400)


def render_neiss():
    st.header("NEISS Emergency Room Injuries")

    conn = get_db()
    if not _table_exists(conn, "neiss_injuries"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Injuries by Year")
        df = run_query("""
            SELECT neiss_year, COUNT(*) as count, SUM(weight) as national_estimate
            FROM neiss_injuries WHERE neiss_year IS NOT NULL
            GROUP BY neiss_year ORDER BY neiss_year
        """)
        if not df.empty:
            fig = px.bar(df, x="neiss_year", y="national_estimate",
                        labels={"neiss_year": "Year", "national_estimate": "National Estimate"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top Diagnoses")
        df = run_query("""
            SELECT diagnosis_name, COUNT(*) as count FROM neiss_injuries
            WHERE diagnosis_name != '' AND diagnosis_name IS NOT NULL
            GROUP BY diagnosis_name ORDER BY count DESC LIMIT 10
        """)
        if not df.empty:
            fig = px.bar(df, x="count", y="diagnosis_name", orientation="h",
                        labels={"diagnosis_name": "Diagnosis", "count": "Cases"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Disposition (Outcome)")
        df = run_query("""
            SELECT disposition_name, COUNT(*) as count FROM neiss_injuries
            WHERE disposition_name != '' AND disposition_name IS NOT NULL
            GROUP BY disposition_name ORDER BY count DESC
        """)
        if not df.empty:
            fig = px.pie(df, values="count", names="disposition_name")
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("Location of Injury")
        df = run_query("""
            SELECT location_name, COUNT(*) as count FROM neiss_injuries
            WHERE location_name != '' AND location_name IS NOT NULL AND location_name != 'Not Recorded'
            GROUP BY location_name ORDER BY count DESC
        """)
        if not df.empty:
            fig = px.pie(df, values="count", names="location_name")
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    # Age distribution
    st.subheader("Age Distribution of Injuries")
    df = run_query("""
        SELECT age, COUNT(*) as count FROM neiss_injuries
        WHERE age IS NOT NULL AND age >= 0 AND age <= 100
        GROUP BY age ORDER BY age
    """)
    if not df.empty:
        fig = px.bar(df, x="age", y="count", labels={"age": "Age", "count": "Injuries"})
        fig.update_layout(template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)


def render_violations():
    st.header("Import Violations")

    conn = get_db()
    if not _table_exists(conn, "import_violations"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Violations by Country")
        df = run_query("""
            SELECT country, COUNT(*) as count FROM import_violations
            WHERE country IS NOT NULL AND country != ''
            GROUP BY UPPER(country) ORDER BY count DESC LIMIT 15
        """)
        if not df.empty:
            fig = px.bar(df, x="count", y="country", orientation="h",
                        labels={"country": "Country", "count": "Violations"})
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Violation Types")
        df = run_query("""
            SELECT violation_type, COUNT(*) as count FROM import_violations
            WHERE violation_type IS NOT NULL AND violation_type != ''
            GROUP BY violation_type ORDER BY count DESC LIMIT 10
        """)
        if not df.empty:
            fig = px.pie(df, values="count", names="violation_type")
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    # CBP actions
    st.subheader("CBP Actions Taken")
    df = run_query("""
        SELECT cbp_action, COUNT(*) as count FROM import_violations
        WHERE cbp_action IS NOT NULL AND cbp_action != ''
        GROUP BY cbp_action ORDER BY count DESC
    """)
    if not df.empty:
        fig = px.bar(df, x="cbp_action", y="count",
                    labels={"cbp_action": "CBP Action", "count": "Count"})
        fig.update_layout(template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

    # Top violating firms
    st.subheader("Top Violating Firms")
    df = run_query("""
        SELECT firm_name, COUNT(*) as violations, country
        FROM import_violations
        GROUP BY normalized_firm ORDER BY violations DESC LIMIT 20
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)


def render_data_explorer():
    st.header("Data Explorer")

    conn = get_db()
    available_tables = [
        t for t in ["recalls", "incidents", "penalties", "import_violations",
                     "manufacturer_profiles", "neiss_injuries", "cross_links"]
        if _table_exists(conn, t)
    ]
    if not available_tables:
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    table = st.selectbox("Select table", [
        "recalls", "incidents", "penalties", "import_violations",
        "manufacturer_profiles", "neiss_injuries", "cross_links"
    ])

    search = st.text_input("Search (searches text columns)", "")

    # All queries use parameterized ? placeholders to prevent SQL injection
    QUERIES = {
        "recalls": {
            "cols": "recall_number, recall_date, title, manufacturer_names, hazard_description, number_of_units, quality_score",
            "search_cols": ["title", "manufacturer_names"],
            "order": "recall_date DESC",
        },
        "incidents": {
            "cols": "report_number, report_date, product_category, manufacturer_name, severity, state, quality_score",
            "search_cols": ["product_description", "manufacturer_name"],
            "order": "report_date DESC",
        },
        "penalties": {
            "cols": "penalty_id, firm_name, penalty_type, penalty_date, fine_amount, act, recall_number",
            "search_cols": ["firm_name"],
            "order": "fine_amount DESC",
        },
        "import_violations": {
            "cols": "nov_date, product_name, violation_type, citation, firm_name, country, cbp_action",
            "search_cols": ["firm_name", "product_name"],
            "order": "nov_date DESC",
        },
        "manufacturer_profiles": {
            "cols": "manufacturer_name, total_recalls, total_incidents, total_penalties, total_fines, total_import_violations, compliance_score, risk_tier",
            "search_cols": ["manufacturer_name"],
            "order": "compliance_score ASC",
        },
        "neiss_injuries": {
            "cols": "cpsc_case_number, treatment_date, age, sex, body_part_name, diagnosis_name, disposition_name, product_1_name, narrative",
            "search_cols": ["narrative"],
            "order": "treatment_date DESC",
        },
    }

    if table in QUERIES:
        q = QUERIES[table]
        if search:
            where = " OR ".join(f"{c} LIKE ?" for c in q["search_cols"])
            params = [f"%{search}%"] * len(q["search_cols"])
            df = run_query(f"SELECT {q['cols']} FROM {table} WHERE {where} ORDER BY {q['order']} LIMIT 500", params)
        else:
            df = run_query(f"SELECT {q['cols']} FROM {table} ORDER BY {q['order']} LIMIT 500")
    else:
        df = run_query("SELECT * FROM cross_links LIMIT 500")

    if not df.empty:
        st.dataframe(df, use_container_width=True, height=500)
        csv = df.to_csv(index=False)
        st.download_button("Download CSV", csv, f"{table}.csv", "text/csv")
    else:
        st.info("No data found")


if __name__ == "__main__":
    main()
