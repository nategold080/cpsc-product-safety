"""Click CLI for CPSC Product Safety Tracker."""

import logging
from pathlib import Path

import click

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """CPSC Product Safety Tracker CLI."""
    pass


@cli.command()
def init():
    """Initialize the database."""
    from .storage.database import init_db
    init_db()
    click.echo("Database initialized.")


@cli.command("scrape-recalls")
def scrape_recalls():
    """Download and import recall data from CPSC API."""
    from .storage.database import get_connection, init_db, upsert_recalls_batch
    from .scrapers.recalls import download_recalls_api, parse_recall_api_record

    init_db()
    raw_records = download_recalls_api()
    click.echo(f"Downloaded {len(raw_records)} raw recalls")

    parsed = [parse_recall_api_record(r) for r in raw_records]
    click.echo(f"Parsed {len(parsed)} recalls")

    conn, owned = get_connection()
    try:
        # Batch in chunks
        chunk_size = 1000
        for i in range(0, len(parsed), chunk_size):
            chunk = parsed[i:i + chunk_size]
            upsert_recalls_batch(chunk, conn=conn)
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM recalls").fetchone()[0]
        click.echo(f"Database now has {count:,} recalls")
    finally:
        if owned:
            conn.close()


@cli.command("scrape-incidents")
def scrape_incidents():
    """Download and import incident reports from SaferProducts.gov."""
    from .storage.database import get_connection, init_db, upsert_incidents_batch
    from .scrapers.incidents import download_incidents, parse_incidents

    init_db()
    csv_path = download_incidents()
    records = parse_incidents(csv_path)
    click.echo(f"Parsed {len(records)} incidents")

    conn, owned = get_connection()
    try:
        chunk_size = 5000
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            upsert_incidents_batch(chunk, conn=conn)
            click.echo(f"  Imported {min(i + chunk_size, len(records)):,}/{len(records):,}")
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
        click.echo(f"Database now has {count:,} incidents")
    finally:
        if owned:
            conn.close()


@cli.command("scrape-neiss")
@click.option("--years", default="2019-2024", help="Year range (e.g., 2019-2024)")
def scrape_neiss(years):
    """Download and import NEISS injury data."""
    from .storage.database import get_connection, init_db, insert_neiss_batch
    from .scrapers.neiss import download_neiss, parse_neiss_tsv

    init_db()

    # Parse year range
    if "-" in years:
        start, end = years.split("-")
        year_list = list(range(int(start), int(end) + 1))
    else:
        year_list = [int(y) for y in years.split(",")]

    paths = download_neiss(year_list)
    click.echo(f"Downloaded {len(paths)} NEISS files")

    conn, owned = get_connection()
    try:
        total = 0
        for path in paths:
            records = parse_neiss_tsv(path)
            chunk_size = 10000
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                insert_neiss_batch(chunk, conn=conn)
            total += len(records)
            click.echo(f"  Imported {len(records):,} from {path}")
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM neiss_injuries").fetchone()[0]
        click.echo(f"Database now has {count:,} NEISS records (imported {total:,})")
    finally:
        if owned:
            conn.close()


@cli.command("scrape-penalties")
def scrape_penalties():
    """Download and import penalty data from CPSC API."""
    from .storage.database import get_connection, init_db, upsert_penalties_batch
    from .scrapers.penalties import download_penalties, parse_all_penalties

    init_db()
    civil_raw, criminal_raw = download_penalties()
    records = parse_all_penalties(civil_raw, criminal_raw)
    click.echo(f"Parsed {len(records)} penalties")

    conn, owned = get_connection()
    try:
        upsert_penalties_batch(records, conn=conn)
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM penalties").fetchone()[0]
        click.echo(f"Database now has {count:,} penalties")
    finally:
        if owned:
            conn.close()


@cli.command("scrape-violations")
def scrape_violations():
    """Download and import import violation data."""
    from .storage.database import get_connection, init_db, insert_import_violations_batch
    from .scrapers.violations import download_violations, parse_violations

    init_db()
    path = download_violations()
    if not path:
        click.echo("Could not download violations file. Skipping.")
        return

    records = parse_violations(path)
    click.echo(f"Parsed {len(records)} import violations")

    conn, owned = get_connection()
    try:
        chunk_size = 5000
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            insert_import_violations_batch(chunk, conn=conn)
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM import_violations").fetchone()[0]
        click.echo(f"Database now has {count:,} import violations")
    finally:
        if owned:
            conn.close()


@cli.command("scrape-fda")
@click.option("--max-events", default=20000, help="Max events per search query")
@click.option("--max-recalls", default=60000, help="Max device recalls")
def scrape_fda(max_events, max_recalls):
    """Download FDA adverse events and device recalls from OpenFDA API."""
    from .storage.database import get_connection, init_db, upsert_fda_events_batch, upsert_fda_recalls_batch
    from .scrapers.fda_downloader import download_device_events, download_device_recalls

    init_db()
    conn, owned = get_connection()
    try:
        # Device adverse events
        click.echo("Downloading FDA device adverse events...")
        events = download_device_events(max_per_search=max_events)
        click.echo(f"Downloaded {len(events):,} FDA adverse events")
        if events:
            for i in range(0, len(events), 5000):
                upsert_fda_events_batch(events[i:i + 5000], conn=conn)
            conn.commit()

        # Device recalls
        click.echo("Downloading FDA device recalls...")
        recalls = download_device_recalls(max_records=max_recalls)
        click.echo(f"Downloaded {len(recalls):,} FDA device recalls")
        if recalls:
            for i in range(0, len(recalls), 5000):
                upsert_fda_recalls_batch(recalls[i:i + 5000], conn=conn)
            conn.commit()

        event_count = conn.execute("SELECT COUNT(*) FROM fda_adverse_events").fetchone()[0]
        recall_count = conn.execute("SELECT COUNT(*) FROM fda_device_recalls").fetchone()[0]
        click.echo(f"Database now has {event_count:,} FDA events, {recall_count:,} FDA recalls")
    finally:
        if owned:
            conn.close()


@cli.command("load-product-codes")
def load_product_codes():
    """Load NEISS product codes from YAML config and update NEISS records."""
    from .storage.database import init_db, load_product_codes_from_yaml, update_neiss_product_names

    init_db()
    yaml_path = str(Path(__file__).parent.parent / "config" / "neiss_product_codes.yaml")
    count = load_product_codes_from_yaml(yaml_path)
    click.echo(f"Loaded {count:,} product codes")

    updated = update_neiss_product_names()
    click.echo(f"Updated {updated:,} NEISS records with product names")


@cli.command("crosslink")
def crosslink():
    """Build cross-links, manufacturer profiles, and hazard validation."""
    from .storage.database import get_connection, init_db
    from .normalization.cross_linker import build_cross_links, build_manufacturer_profiles
    from .validation.hazard_validator import build_hazard_validation, load_hazard_map_to_db

    init_db()
    conn, owned = get_connection()
    try:
        links = build_cross_links(conn)
        click.echo(f"Created {links:,} cross-links")

        profiles = build_manufacturer_profiles(conn)
        click.echo(f"Built {profiles:,} manufacturer profiles")

        # Hazard validation
        yaml_path = str(Path(__file__).parent.parent / "config" / "hazard_diagnosis_map.yaml")
        if Path(yaml_path).exists():
            mappings = load_hazard_map_to_db(conn, yaml_path)
            click.echo(f"Loaded {mappings:,} hazard-diagnosis mappings")
            validations = build_hazard_validation(conn)
            click.echo(f"Built {validations:,} hazard validation records")
    finally:
        if owned:
            conn.close()


@cli.command("export")
def export():
    """Generate CSV, JSON, and Markdown exports."""
    from .storage.database import get_connection, init_db
    from .export.exporter import export_all

    init_db()
    conn, owned = get_connection()
    try:
        counts = export_all(conn)
        click.echo("Exports complete:")
        for name, count in counts.items():
            click.echo(f"  {name}: {count:,}")
    finally:
        if owned:
            conn.close()


@cli.command("stats")
def stats():
    """Show database statistics."""
    from .storage.database import get_stats

    s = get_stats()
    click.echo("\n=== CPSC Product Safety Database Statistics ===\n")
    for key, val in s.items():
        if isinstance(val, float):
            click.echo(f"  {key}: {val:,.1f}")
        elif isinstance(val, int):
            click.echo(f"  {key}: {val:,}")
        else:
            click.echo(f"  {key}: {val}")


@cli.command("pipeline")
@click.option("--skip-neiss", is_flag=True, help="Skip NEISS download (large)")
def pipeline(skip_neiss):
    """Run the full pipeline: scrape → crosslink → export."""
    from .storage.database import get_connection, init_db, upsert_recalls_batch, upsert_incidents_batch
    from .storage.database import upsert_penalties_batch, insert_neiss_batch, insert_import_violations_batch
    from .scrapers.recalls import download_recalls_api, parse_recall_api_record
    from .scrapers.incidents import download_incidents, parse_incidents
    from .scrapers.penalties import download_penalties, parse_all_penalties
    from .scrapers.neiss import download_neiss, parse_neiss_tsv
    from .scrapers.violations import download_violations, parse_violations
    from .normalization.cross_linker import build_cross_links, build_manufacturer_profiles
    from .export.exporter import export_all

    init_db()
    conn, owned = get_connection()

    try:
        # 1. Recalls
        click.echo("\n--- Stage 1: Recalls ---")
        raw = download_recalls_api()
        parsed = [parse_recall_api_record(r) for r in raw]
        for i in range(0, len(parsed), 1000):
            upsert_recalls_batch(parsed[i:i+1000], conn=conn)
        conn.commit()
        click.echo(f"Imported {len(parsed):,} recalls")

        # 2. Incidents
        click.echo("\n--- Stage 2: Incidents ---")
        csv_path = download_incidents()
        incidents = parse_incidents(csv_path)
        for i in range(0, len(incidents), 5000):
            upsert_incidents_batch(incidents[i:i+5000], conn=conn)
        conn.commit()
        click.echo(f"Imported {len(incidents):,} incidents")

        # 3. Penalties
        click.echo("\n--- Stage 3: Penalties ---")
        civil, criminal = download_penalties()
        penalties = parse_all_penalties(civil, criminal)
        upsert_penalties_batch(penalties, conn=conn)
        conn.commit()
        click.echo(f"Imported {len(penalties):,} penalties")

        # 4. NEISS
        if not skip_neiss:
            click.echo("\n--- Stage 4: NEISS Injuries ---")
            paths = download_neiss()
            total_neiss = 0
            for path in paths:
                records = parse_neiss_tsv(path)
                for i in range(0, len(records), 10000):
                    insert_neiss_batch(records[i:i+10000], conn=conn)
                total_neiss += len(records)
            conn.commit()
            click.echo(f"Imported {total_neiss:,} NEISS records")
        else:
            click.echo("\n--- Stage 4: NEISS Injuries (SKIPPED) ---")

        # 5. Import Violations
        click.echo("\n--- Stage 5: Import Violations ---")
        viol_path = download_violations()
        if viol_path:
            violations = parse_violations(viol_path)
            for i in range(0, len(violations), 5000):
                insert_import_violations_batch(violations[i:i+5000], conn=conn)
            conn.commit()
            click.echo(f"Imported {len(violations):,} import violations")
        else:
            click.echo("Import violations file not available. Skipping.")

        # 5b. Load product codes
        click.echo("\n--- Stage 5b: Product Code Resolution ---")
        from .storage.database import load_product_codes_from_yaml, update_neiss_product_names
        yaml_path = str(Path(__file__).parent.parent / "config" / "neiss_product_codes.yaml")
        if Path(yaml_path).exists():
            pc_count = load_product_codes_from_yaml(yaml_path, conn=conn)
            conn.commit()
            updated = update_neiss_product_names(conn=conn)
            conn.commit()
            click.echo(f"Loaded {pc_count:,} product codes, updated {updated:,} NEISS records")
        else:
            click.echo("Product codes YAML not found, skipping")

        # 6. Cross-link
        click.echo("\n--- Stage 6: Cross-Linking ---")
        links = build_cross_links(conn)
        click.echo(f"Created {links:,} cross-links")

        profiles = build_manufacturer_profiles(conn)
        click.echo(f"Built {profiles:,} manufacturer profiles")

        # 7. Export
        click.echo("\n--- Stage 7: Export ---")
        counts = export_all(conn)
        for name, count in counts.items():
            click.echo(f"  {name}: {count:,}")

        click.echo("\n=== Pipeline complete! ===")

    finally:
        if owned:
            conn.close()


@cli.command("dashboard")
def dashboard():
    """Launch the Streamlit dashboard."""
    import subprocess
    import sys
    dash_path = str(Path(__file__).parent / "dashboard" / "app.py")
    subprocess.run([sys.executable, "-m", "streamlit", "run", dash_path])


if __name__ == "__main__":
    cli()
