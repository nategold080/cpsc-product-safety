"""Quality scoring for CPSC Product Safety Tracker records."""


def score_recall(record: dict) -> float:
    """Score a recall record (0.0-1.0) based on field completeness.

    Weighted components:
    - recall_number (10%): Unique identifier
    - recall_date (10%): Timing
    - title (10%): Basic description
    - description (10%): Detailed description
    - product_names (10%): Product identification
    - hazard_description (15%): Safety hazard
    - manufacturer_names (15%): Responsible party
    - number_of_units (10%): Scale of recall
    - remedy_description (5%): Consumer action
    - manufacturer_countries (5%): Origin
    """
    weights = {
        "recall_number": 0.10,
        "recall_date": 0.10,
        "title": 0.10,
        "description": 0.10,
        "product_names": 0.10,
        "hazard_description": 0.15,
        "manufacturer_names": 0.15,
        "number_of_units": 0.10,
        "remedy_description": 0.05,
        "manufacturer_countries": 0.05,
    }
    total = 0.0
    for field, weight in weights.items():
        val = record.get(field)
        if val and str(val).strip():
            total += weight
    return round(total, 3)


def score_incident(record: dict) -> float:
    """Score an incident record (0.0-1.0) based on field completeness.

    Weighted components:
    - report_number (10%): Unique identifier
    - report_date (10%): Timing
    - product_description (10%): Product identification
    - product_category (10%): Classification
    - manufacturer_name (15%): Responsible party
    - incident_description (15%): Event details
    - state (5%): Geographic
    - severity (10%): Outcome severity
    - product_code (5%): CPSC code
    - brand_name (5%): Brand identification
    - model_name (5%): Model identification
    """
    weights = {
        "report_number": 0.10,
        "report_date": 0.10,
        "product_description": 0.10,
        "product_category": 0.10,
        "manufacturer_name": 0.15,
        "incident_description": 0.15,
        "state": 0.05,
        "severity": 0.10,
        "product_code": 0.05,
        "brand_name": 0.05,
        "model_name": 0.05,
    }
    total = 0.0
    for field, weight in weights.items():
        val = record.get(field)
        if val and str(val).strip():
            total += weight
    return round(total, 3)


def score_neiss(record: dict) -> float:
    """Score a NEISS injury record (0.0-1.0) based on field completeness.

    Weighted components:
    - cpsc_case_number (10%)
    - treatment_date (10%)
    - age (10%)
    - sex (5%)
    - body_part (10%)
    - diagnosis (10%)
    - disposition (10%)
    - product_1 (15%)
    - narrative (15%)
    - weight (5%)
    """
    weights = {
        "cpsc_case_number": 0.10,
        "treatment_date": 0.10,
        "age": 0.10,
        "sex": 0.05,
        "body_part": 0.10,
        "diagnosis": 0.10,
        "disposition": 0.10,
        "product_1": 0.15,
        "narrative": 0.15,
        "weight": 0.05,
    }
    total = 0.0
    for field, weight in weights.items():
        val = record.get(field)
        if val is not None and str(val).strip() and str(val) != "0":
            total += weight
    return round(total, 3)


def score_penalty(record: dict) -> float:
    """Score a penalty record (0.0-1.0) based on field completeness.

    Weighted components:
    - penalty_id (10%)
    - firm_name (20%)
    - penalty_type (10%)
    - penalty_date (10%)
    - fine_amount (20%)
    - act (10%)
    - recall_number (10%)
    - release_title (10%)
    """
    weights = {
        "penalty_id": 0.10,
        "firm_name": 0.20,
        "penalty_type": 0.10,
        "penalty_date": 0.10,
        "fine_amount": 0.20,
        "act": 0.10,
        "recall_number": 0.10,
        "release_title": 0.10,
    }
    total = 0.0
    for field, weight in weights.items():
        val = record.get(field)
        if val is not None and str(val).strip() and str(val) != "0":
            total += weight
    return round(total, 3)


def score_import_violation(record: dict) -> float:
    """Score an import violation record (0.0-1.0) based on field completeness.

    Weighted components:
    - nov_date (10%)
    - product_name (15%)
    - violation_type (15%)
    - citation (10%)
    - firm_name (20%)
    - country (10%)
    - domestic_action (10%)
    - cbp_action (10%)
    """
    weights = {
        "nov_date": 0.10,
        "product_name": 0.15,
        "violation_type": 0.15,
        "citation": 0.10,
        "firm_name": 0.20,
        "country": 0.10,
        "domestic_action": 0.10,
        "cbp_action": 0.10,
    }
    total = 0.0
    for field, weight in weights.items():
        val = record.get(field)
        if val and str(val).strip():
            total += weight
    return round(total, 3)


def score_fda_event(record: dict) -> float:
    """Score an FDA adverse event record (0.0-1.0)."""
    weights = {
        "event_id": 0.10,
        "report_date": 0.10,
        "product_name": 0.15,
        "manufacturer_name": 0.20,
        "event_type": 0.15,
        "patient_outcome": 0.10,
        "description": 0.10,
        "source": 0.10,
    }
    total = 0.0
    for field, weight in weights.items():
        val = record.get(field)
        if val and str(val).strip():
            total += weight
    return round(total, 3)


def score_fda_recall(record: dict) -> float:
    """Score an FDA device recall record (0.0-1.0)."""
    weights = {
        "recall_id": 0.10,
        "product_description": 0.15,
        "reason_for_recall": 0.20,
        "manufacturer_name": 0.20,
        "recall_class": 0.10,
        "recall_status": 0.10,
        "event_date": 0.15,
    }
    total = 0.0
    for field, weight in weights.items():
        val = record.get(field)
        if val and str(val).strip():
            total += weight
    return round(total, 3)


def compute_compliance_score(profile: dict) -> float:
    """Compute manufacturer compliance score (0.0-1.0). Lower = worse compliance.

    Components:
    - Recall frequency (30%): inverse of recall count (0 recalls = 1.0)
    - Penalty severity (25%): inverse of total fines
    - Incident volume (25%): inverse of incident count
    - Import violations (20%): inverse of violation count

    Only active components contribute (weight rebalanced).
    """
    components = {}
    active_weights = {}

    # Recall frequency component
    recalls = profile.get("total_recalls", 0)
    if recalls == 0:
        components["recalls"] = 1.0
    elif recalls <= 2:
        components["recalls"] = 0.7
    elif recalls <= 5:
        components["recalls"] = 0.4
    elif recalls <= 10:
        components["recalls"] = 0.2
    else:
        components["recalls"] = 0.05
    active_weights["recalls"] = 0.30

    # Penalty severity
    fines = profile.get("total_fines", 0.0)
    if fines == 0:
        components["penalties"] = 1.0
    elif fines < 100_000:
        components["penalties"] = 0.7
    elif fines < 1_000_000:
        components["penalties"] = 0.4
    elif fines < 10_000_000:
        components["penalties"] = 0.2
    else:
        components["penalties"] = 0.05
    active_weights["penalties"] = 0.25

    # Incident volume
    incidents = profile.get("total_incidents", 0)
    if incidents == 0:
        components["incidents"] = 1.0
    elif incidents <= 5:
        components["incidents"] = 0.7
    elif incidents <= 20:
        components["incidents"] = 0.4
    elif incidents <= 50:
        components["incidents"] = 0.2
    else:
        components["incidents"] = 0.05
    active_weights["incidents"] = 0.25

    # Import violations
    violations = profile.get("total_import_violations", 0)
    if violations == 0:
        components["violations"] = 1.0
    elif violations <= 5:
        components["violations"] = 0.7
    elif violations <= 20:
        components["violations"] = 0.4
    elif violations <= 50:
        components["violations"] = 0.2
    else:
        components["violations"] = 0.05
    active_weights["violations"] = 0.20

    weight_sum = sum(active_weights.values())
    if weight_sum == 0:
        return 1.0

    score = sum(components[k] * active_weights[k] for k in components) / weight_sum
    return round(score, 3)


def assign_risk_tier(score: float) -> str:
    """Assign risk tier based on compliance score."""
    if score >= 0.8:
        return "LOW"
    elif score >= 0.5:
        return "MEDIUM"
    elif score >= 0.3:
        return "HIGH"
    else:
        return "CRITICAL"
