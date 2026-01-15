# -------------------------------------------------------------
# GapCoder v1.3 — GAP Analysis Summariser (Importance / Client / Competitor + Comments)
#
# Primary column format:
#   gap_imp_1, gap_perf_1, gap_comp_1, gap_comm_1
# Backward compatible:
#   gap1_imp, gap1_perf, gap1_comp, gap1_comm
#
# Input methods:
# - Upload .xlsx (recommended when comments are long / may contain line breaks)
# - Paste TSV/CSV copied from Excel
#
# Gap Dictionary (required):
#   1 = Section | Gap name
#
# Missing rules:
# - 999 = don't know  -> treated as missing
# - 0   = no answer   -> treated as missing
# - blank = missing
# - If competitor missing but imp/perf exist -> "competitor refused" (row-level signal)
# - If imp+perf+comp all missing -> "not asked to respondent" (row-level signal)
# -------------------------------------------------------------

import streamlit as st
import os
import json
import re
import csv
from io import StringIO, BytesIO
from datetime import datetime
from anthropic import Anthropic

try:
    from openpyxl import load_workbook
    XLSX_OK = True
except Exception:
    XLSX_OK = False


# --------------------
# Config
# --------------------
CLAUDE_MODEL = "claude-sonnet-4-5"
MAX_CLAUDE_TOKENS = 2800
PROJECTS_FILE = "gapcoder_projects.json"
SAME_TOL = 0.05

# NEW format: gap_imp_1
GAP_COL_RE_NEW = re.compile(r"^gap_(imp|perf|comp|comm)_(\d+)$", re.IGNORECASE)
# OLD format: gap1_imp
GAP_COL_RE_OLD = re.compile(r"^gap(\d+)_(imp|perf|comp|comm)$", re.IGNORECASE)

RESP_ID_CANDIDATES = {"resp_id", "respondent_id", "respondent", "resp", "id"}
ID_LIKE_RE = re.compile(r"^[A-Za-z]{2,}\d+.*$")  # e.g. UPM001, RESP_001, etc.


# --------------------
# Project persistence
# --------------------
def load_projects(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_projects(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# --------------------
# Parsing helpers
# --------------------
def detect_delimiter_from_header(header_line: str) -> str:
    # Best-effort, keeps it simple
    if "\t" in header_line:
        return "\t"
    if ";" in header_line and "," not in header_line:
        return ";"
    return ","

def normalise_headers(fieldnames):
    """Replace empty headers with COL_1, COL_2 etc., and make them unique."""
    seen = set()
    out = []
    for idx, h in enumerate(fieldnames):
        name = "" if h is None else str(h).strip()
        if name == "":
            name = f"COL_{idx+1}"
        base = name
        i = 2
        while name in seen:
            name = f"{base}_{i}"
            i += 1
        seen.add(name)
        out.append(name)
    return out

def parse_table_paste(text: str):
    if not text or not text.strip():
        raise ValueError("No data pasted.")

    # Remove fully empty lines (common when pasting)
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if len(lines) < 2:
        raise ValueError(
            "It looks like you only pasted one line (likely just the header, or header+values without row breaks). "
            "Try copying the full Excel table again, or use the .xlsx upload option."
        )

    delim = detect_delimiter_from_header(lines[0])
    cleaned_text = "\n".join(lines)
    f = StringIO(cleaned_text)
    reader = csv.reader(f, delimiter=delim)

    raw_fieldnames = next(reader, None)
    if not raw_fieldnames:
        raise ValueError("Could not read header row. Make sure the first line is the header.")

    headers = normalise_headers(raw_fieldnames)

    rows = []
    for row in reader:
        # Skip completely empty rows
        if not any(str(v).strip() for v in row if v is not None):
            continue
        # Pad / trim to header length
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        if len(row) > len(headers):
            row = row[:len(headers)]
        rows.append({headers[i]: ("" if row[i] is None else str(row[i]).strip()) for i in range(len(headers))})

    if not rows:
        raise ValueError(
            "No rows found under the header. This often happens when Excel comments contain hidden line breaks, "
            "or the paste didn't include any respondent rows. Recommendation: upload the .xlsx instead."
        )

    return headers, rows

def parse_table_xlsx(uploaded_file):
    if not XLSX_OK:
        raise ValueError("openpyxl is not available. Add 'openpyxl' to requirements.txt.")

    data = uploaded_file.getvalue()
    wb = load_workbook(BytesIO(data), data_only=True)
    ws = wb.active

    # Read header row (row 1)
    raw_headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    if not raw_headers or all(h is None or str(h).strip() == "" for h in raw_headers):
        raise ValueError("The first row in the Excel file is empty. Please ensure row 1 contains headers.")

    headers = normalise_headers(raw_headers)

    rows = []
    for r in ws.iter_rows(min_row=2, values_only=True):
        if r is None:
            continue
        if not any(v is not None and str(v).strip() != "" for v in r):
            continue
        row = list(r)
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        if len(row) > len(headers):
            row = row[:len(headers)]

        cleaned = {}
        for i, h in enumerate(headers):
            v = row[i]
            if v is None:
                cleaned[h] = ""
            else:
                cleaned[h] = str(v).strip() if not isinstance(v, (int, float)) else v
        rows.append(cleaned)

    if not rows:
        raise ValueError("No data rows found under the header in the Excel file.")
    return headers, rows

def parse_score(value):
    if value is None:
        return None, "blank"

    # Keep numeric as-is when coming from xlsx
    if isinstance(value, (int, float)):
        num = float(value)
    else:
        s = str(value).strip()
        if s == "":
            return None, "blank"
        try:
            num = float(s.replace(",", "."))
        except:
            return None, "blank"

    if abs(num - 0.0) < 1e-12:
        return None, "no_answer_0"
    if abs(num - 999.0) < 1e-12:
        return None, "dont_know_999"
    return num, None

def parse_gap_dictionary(text: str):
    """
    Expected:
      12 = Section Name | Criterion Name
    Also accepts ":" instead of "=".
    """
    mapping = {}
    if not text or not text.strip():
        return mapping

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if "=" in line:
            left, right = line.split("=", 1)
        elif ":" in line:
            left, right = line.split(":", 1)
        else:
            continue

        try:
            gap_no = int(left.strip())
        except:
            continue

        right = right.strip()
        if "|" in right:
            section, crit = right.split("|", 1)
            section = section.strip() or "General"
            crit = crit.strip() or f"Gap {gap_no}"
        else:
            section = "General"
            crit = right.strip() or f"Gap {gap_no}"

        mapping[gap_no] = {"section": section, "criterion": crit}

    return mapping

def build_gap_schema(headers):
    """
    Returns:
      gaps: sorted list of gap numbers
      col_map: dict[(gap_no, suffix)] -> header
    suffix: imp, perf, comp, comm
    """
    col_map = {}
    gap_nums = set()

    for h in headers:
        hh = str(h).strip()

        m_new = GAP_COL_RE_NEW.match(hh)
        if m_new:
            suffix = m_new.group(1).lower()
            n = int(m_new.group(2))
            col_map[(n, suffix)] = h
            gap_nums.add(n)
            continue

        m_old = GAP_COL_RE_OLD.match(hh)
        if m_old:
            n = int(m_old.group(1))
            suffix = m_old.group(2).lower()
            col_map.setdefault((n, suffix), h)
            gap_nums.add(n)

    return sorted(gap_nums), col_map

def detect_resp_id_col(headers, rows):
    # 1) by name
    for h in headers:
        if str(h).strip().lower() in RESP_ID_CANDIDATES:
            return h

    # 2) heuristic: look for a column that is mostly "id-like"
    best = None
    best_score = 0.0
    for h in headers:
        vals = []
        for r in rows[:50]:
            vals.append(r.get(h, ""))
        nonempty = [v for v in vals if str(v).strip() != ""]
        if len(nonempty) < 5:
            continue
        idlike = sum(1 for v in nonempty if ID_LIKE_RE.match(str(v).strip()))
        score = idlike / max(1, len(nonempty))
        if score > best_score:
            best_score = score
            best = h

    if best_score >= 0.6:
        return best
    return None

def mean(vals):
    vv = [v for v in vals if v is not None]
    if not vv:
        return None
    return sum(vv) / len(vv)

def classify_vs_comp(gap):
    if gap is None:
        return None
    if gap > SAME_TOL:
        return "leading"
    if gap < -SAME_TOL:
        return "lagging"
    return "same"

def compute_criterion_table(rows, gaps, col_map, resp_id_col, gap_dict):
    """
    Returns:
      criterion_table: list[dict]
      comments_by_gap: dict gap_no -> list of {"resp_id","comment"} (truncated)
      data_quality: dict
    """
    criterion_table = []
    comments_by_gap = {}
    data_quality = {"note": "0, 999 and blanks treated as missing and ignored in means."}

    # Collect comments by gap (we'll later sample only the important ones)
    for n in gaps:
        comm_col = col_map.get((n, "comm"))
        if not comm_col:
            continue
        bucket = []
        for i, r in enumerate(rows, start=1):
            resp_id = (r.get(resp_id_col) if resp_id_col else None) or f"RESP_{i:03d}"
            c = r.get(comm_col, "")
            c = c if c is not None else ""
            c = str(c).strip()
            if c:
                if len(c) > 280:
                    c = c[:280].rstrip() + "…"
                bucket.append({"resp_id": str(resp_id).strip(), "comment": c})
        if bucket:
            comments_by_gap[n] = bucket

    for n in gaps:
        imp_col = col_map.get((n, "imp"))
        perf_col = col_map.get((n, "perf"))
        comp_col = col_map.get((n, "comp"))

        imp_vals, perf_vals, comp_vals = [], [], []
        rows_total = len(rows)
        rows_not_asked = 0
        rows_comp_refused = 0
        pairs_expect = 0
        pairs_comp = 0

        missing_counts = {
            "Importance": {"blank": 0, "no_answer_0": 0, "dont_know_999": 0},
            "Client": {"blank": 0, "no_answer_0": 0, "dont_know_999": 0},
            "Competitor": {"blank": 0, "no_answer_0": 0, "dont_know_999": 0},
        }

        for r in rows:
            imp, imp_m = parse_score(r.get(imp_col, "") if imp_col else "")
            perf, perf_m = parse_score(r.get(perf_col, "") if perf_col else "")
            comp, comp_m = parse_score(r.get(comp_col, "") if comp_col else "")

            if imp_m: missing_counts["Importance"][imp_m] += 1
            if perf_m: missing_counts["Client"][perf_m] += 1
            if comp_m: missing_counts["Competitor"][comp_m] += 1

            if imp is None and perf is None and comp is None:
                rows_not_asked += 1

            if comp is None and (imp is not None or perf is not None):
                rows_comp_refused += 1

            imp_vals.append(imp)
            perf_vals.append(perf)
            comp_vals.append(comp)

            if imp is not None and perf is not None:
                pairs_expect += 1
            if perf is not None and comp is not None:
                pairs_comp += 1

        imp_mean = mean(imp_vals)
        perf_mean = mean(perf_vals)
        comp_mean = mean(comp_vals)

        gap_expect = (imp_mean - perf_mean) if (imp_mean is not None and perf_mean is not None) else None
        gap_comp = (perf_mean - comp_mean) if (perf_mean is not None and comp_mean is not None) else None

        meta = gap_dict.get(n, {"section": "Unmapped", "criterion": f"Gap {n}"})

        criterion_table.append({
            "gap_no": n,
            "section": meta["section"],
            "criterion": meta["criterion"],

            "rows_total": rows_total,
            "rows_not_asked": rows_not_asked,
            "rows_competitor_refused": rows_comp_refused,

            "mean_importance": None if imp_mean is None else round(imp_mean, 2),
            "mean_client": None if perf_mean is None else round(perf_mean, 2),
            "mean_competitor": None if comp_mean is None else round(comp_mean, 2),

            "mean_gap_vs_expectations": None if gap_expect is None else round(gap_expect, 2),
            "mean_gap_vs_competitor": None if gap_comp is None else round(gap_comp, 2),

            "valid_pairs_vs_expectations": pairs_expect,
            "valid_pairs_vs_competitor": pairs_comp,

            "missing_counts": missing_counts,
        })

    return criterion_table, comments_by_gap, data_quality

def summarise(criteria_rows):
    total = len(criteria_rows)
    eval_comp = [r for r in criteria_rows if r["mean_gap_vs_competitor"] is not None]
    eval_expect = [r for r in criteria_rows if r["mean_gap_vs_expectations"] is not None]

    lead = same = lag = 0
    for r in eval_comp:
        cls = classify_vs_comp(r["mean_gap_vs_competitor"])
        if cls == "leading": lead += 1
        elif cls == "same": same += 1
        elif cls == "lagging": lag += 1

    top_leading = sorted(eval_comp, key=lambda r: r["mean_gap_vs_competitor"], reverse=True)[:3]
    top_lagging = sorted(eval_comp, key=lambda r: r["mean_gap_vs_competitor"])[:3]
    top_expect_gaps = sorted(eval_expect, key=lambda r: r["mean_gap_vs_expectations"], reverse=True)[:3]

    avg_imp = mean([r["mean_importance"] for r in criteria_rows])
    avg_cli = mean([r["mean_client"] for r in criteria_rows])
    avg_com = mean([r["mean_competitor"] for r in criteria_rows])
    avg_gap_comp = mean([r["mean_gap_vs_competitor"] for r in eval_comp])
    avg_gap_expect = mean([r["mean_gap_vs_expectations"] for r in eval_expect])

    rows_comp_refused_total = sum(r["rows_competitor_refused"] for r in criteria_rows)
    rows_not_asked_total = sum(r["rows_not_asked"] for r in criteria_rows)

    return {
        "criteria_total": total,
        "criteria_evaluable_vs_competitor": len(eval_comp),
        "criteria_evaluable_vs_expectations": len(eval_expect),
        "counts_vs_competitor": {"leading": lead, "same": same, "lagging": lag},
        "averages": {
            "importance": None if avg_imp is None else round(avg_imp, 2),
            "client": None if avg_cli is None else round(avg_cli, 2),
            "competitor": None if avg_com is None else round(avg_com, 2),
            "gap_vs_competitor": None if avg_gap_comp is None else round(avg_gap_comp, 2),
            "gap_vs_expectations": None if avg_gap_expect is None else round(avg_gap_expect, 2),
        },
        "top3_leading_vs_competitor": [
            {"gap_no": r["gap_no"], "section": r["section"], "criterion": r["criterion"], "gap": r["mean_gap_vs_competitor"]}
            for r in top_leading
        ],
        "top3_lagging_vs_competitor": [
            {"gap_no": r["gap_no"], "section": r["section"], "criterion": r["criterion"], "gap": r["mean_gap_vs_competitor"]}
            for r in top_lagging
        ],
        "top3_gaps_vs_expectations": [
            {"gap_no": r["gap_no"], "section": r["section"], "criterion": r["criterion"], "gap": r["mean_gap_vs_expectations"]}
            for r in top_expect_gaps
        ],
        "data_quality_signals": {
            "respondent_rows_competitor_refused_total": rows_comp_refused_total,
            "respondent_rows_not_asked_total": rows_not_asked_total,
            "note": "0, 999 and blanks are treated as missing and ignored in means."
        }
    }

def pick_comment_samples(criteria_table, comments_by_gap, max_gaps=12, per_gap=6):
    """
    Keep prompt small:
    - focus on biggest gaps vs expectations + most lagging vs competitor
    - sample a few comments per priority gap
    """
    eval_expect = [r for r in criteria_table if r["mean_gap_vs_expectations"] is not None]
    eval_comp = [r for r in criteria_table if r["mean_gap_vs_competitor"] is not None]

    top_expect = sorted(eval_expect, key=lambda r: r["mean_gap_vs_expectations"], reverse=True)[:6]
    top_lag = sorted(eval_comp, key=lambda r: r["mean_gap_vs_competitor"])[:6]

    priority = []
    seen = set()
    for r in top_expect + top_lag:
        n = r["gap_no"]
        if n not in seen:
            priority.append(n)
            seen.add(n)
    priority = priority[:max_gaps]

    out = {}
    for n in priority:
        bucket = comments_by_gap.get(n, [])
        if bucket:
            out[n] = bucket[:per_gap]
    return out

def build_claude_prompt(ctx, criteria_table, overall_stats, section_stats, comment_samples, data_quality):
    json_template = {
        "total_gap_overview": {"slide_bullets": ["..."], "narrative": "..."},
        "sections": [{"section": "Section name", "slide_bullets": ["..."], "narrative": "..."}]
    }

    prompt = f"""
You are a senior market research consultant. Write a GAP analysis summary that is slide-ready and grounded in the provided stats and comment samples.

CRITICAL RULES:
- Do NOT invent numbers.
- Use provided stats for totals/counts/top gaps.
- Mention briefly if competitor benchmarking is limited (missing/refused) or if some gaps were not asked.
- Output MUST be valid JSON only (no backticks, no extra commentary) matching the JSON template.

PROJECT CONTEXT:
- Project: {ctx.get("project_no","")}
- Client: {ctx.get("client_name","")}
- Industry: {ctx.get("industry","")}
- Objectives: {ctx.get("objectives","")}

DEFINITIONS:
- Gap vs Competitor = mean_client - mean_competitor (positive = client leads)
- Gap vs Expectations = mean_importance - mean_client (positive = client under-delivers vs what matters)
- Missing handling: 0, 999 and blanks were ignored in means.

DATA QUALITY NOTE:
{json.dumps(data_quality, ensure_ascii=False, indent=2)}

OVERALL STATS:
{json.dumps(overall_stats, ensure_ascii=False, indent=2)}

SECTION STATS:
{json.dumps(section_stats, ensure_ascii=False, indent=2)}

CRITERION TABLE:
{json.dumps(criteria_table, ensure_ascii=False, indent=2)}

COMMENT SAMPLES (use for improvement suggestions; cite gap/section where possible):
{json.dumps(comment_samples, ensure_ascii=False, indent=2)}

WHAT TO PRODUCE:
1) Total GAP overview (200–400 words narrative + slide bullets)
   Include:
   - how many criteria in total
   - high-level remark
   - how many leading/same/lagging vs competition (where evaluable)
   - top 3 leading vs competition + biggest gaps vs competition
   - biggest gaps vs expectations
   - main improvement suggestions from customers (from comments)
2) Key findings per section (200–300 words each + slide bullets)
   Cover:
   - expectations met in general
   - top gaps vs expectations
   - performance vs competition
   - top gaps vs competition / where lagging
   - improvement suggestions from comments

OUTPUT FORMAT:
Return JSON matching this template exactly:
{json.dumps(json_template, ensure_ascii=False, indent=2)}
"""
    return prompt.strip()


# --------------------
# Streamlit UI
# --------------------
st.set_page_config(page_title="GapCoder", layout="wide")
st.markdown(f"# 📊 GapCoder (v1.3)\n_Last updated: {datetime.now():%Y-%m-%d}_")

projects = load_projects(PROJECTS_FILE)
client = Anthropic(api_key=st.secrets.get("ANTHROPIC_API_KEY", ""))

with st.expander("1. Project Context", expanded=True):
    sel = st.selectbox("Load project:", ["-- New --"] + list(projects.keys()))
    project_no = sel if sel != "-- New --" else st.text_input("Project Number")
    defaults = projects.get(project_no, {}) if project_no else {}

    c1, c2 = st.columns(2)
    with c1:
        client_name = st.text_input("Client Name", value=defaults.get("client_name", ""))
        industry = st.text_input("Industry", value=defaults.get("industry", ""))
        mode = st.radio("Analysis mode", ["All sections", "One section"], index=0)
    with c2:
        objectives = st.text_area("Project Objectives", value=defaults.get("objectives", ""), height=90)

    gap_dict_raw = st.text_area(
        "Gap Dictionary (required): gap number = Section | Gap name",
        value=defaults.get("gap_dict_raw", ""),
        height=170,
        help="Example:\n1 = Communication | Proactive updates\n2 = Communication | Clarity of information\n6 = Supply Chain | Delivery reliability"
    )

    ctx = {
        "project_no": project_no,
        "client_name": client_name,
        "industry": industry,
        "objectives": objectives,
        "mode": mode,
        "gap_dict_raw": gap_dict_raw
    }

    if project_no:
        projects[project_no] = ctx
        save_projects(PROJECTS_FILE, projects)

    # Small sanity feedback for dictionary
    gd = parse_gap_dictionary(gap_dict_raw)
    if gd:
        secs = sorted({v["section"] for v in gd.values()})
        st.info(f"✅ Gap Dictionary loaded: {len(gd)} gaps mapped across {len(secs)} sections.")
    else:
        st.warning("Gap Dictionary is required before running the analysis.")

with st.expander("2. Input data (simple)", expanded=True):
    st.markdown(
        "**Recommended:** Upload the Excel file (.xlsx). This avoids copy-paste issues when comments contain line breaks.\n\n"
        "Supported headers:\n"
        "- `gap_imp_1`, `gap_perf_1`, `gap_comp_1`, `gap_comm_1` (primary)\n"
        "- `gap1_imp` etc. (fallback)\n\n"
        "Missing rules: blank / `0` / `999` are treated as missing and ignored in means."
    )

    input_mode = st.radio("How do you want to provide data?", ["Upload Excel (.xlsx)", "Paste table (TSV/CSV)"], index=0)

    uploaded = None
    raw_text = ""

    if input_mode == "Upload Excel (.xlsx)":
        if not XLSX_OK:
            st.error("Excel upload needs openpyxl. Add it to requirements.txt and redeploy.")
        uploaded = st.file_uploader("Upload .xlsx", type=["xlsx"])
        st.caption("Tip: make sure row 1 contains headers and each following row is one respondent.")
    else:
        st.code(
            "ID\tgap_imp_1\tgap_perf_1\tgap_comp_1\tgap_comm_1\n"
            "UPM001\t7\t8\t7\t(optional comment)\n"
            "UPM002\t9\t9\t\tRefused competitor rating\n",
            language="text"
        )
        raw_text = st.text_area("Paste table here", height=260, key="raw_gap")

if st.button("🧠 Generate GAP Analysis"):
    if not ctx.get("gap_dict_raw", "").strip():
        st.error("Please fill in the Gap Dictionary first (gap number = Section | Gap name).")
        st.stop()

    gap_dict = parse_gap_dictionary(ctx["gap_dict_raw"])

    # Parse data
    try:
        if input_mode == "Upload Excel (.xlsx)":
            if uploaded is None:
                st.error("Please upload an .xlsx file first.")
                st.stop()
            headers, rows = parse_table_xlsx(uploaded)
        else:
            if not raw_text.strip():
                st.error("Paste your GAP table first (header + rows).")
                st.stop()
            headers, rows = parse_table_paste(raw_text)
    except ValueError as e:
        st.error(str(e))
        st.stop()

    gaps, col_map = build_gap_schema(headers)
    if not gaps:
        st.error("Could not find any gap columns. Expected headers like gap_imp_1 / gap_perf_1 / gap_comp_1 / gap_comm_1.")
        st.stop()

    # Ensure dictionary covers all gaps found
    missing_in_dict = [n for n in gaps if n not in gap_dict]
    if missing_in_dict:
        st.error(f"Gap Dictionary is missing these gap numbers: {missing_in_dict}. Please add them.")
        st.stop()

    resp_id_col = detect_resp_id_col(headers, rows)
    if resp_id_col is None:
        st.warning("No RESP_ID/ID column detected. I will label rows as RESP_001, RESP_002, ...")

    # Optional: choose a section before running
    selected_section = None
    if ctx.get("mode") == "One section":
        all_sections = sorted({gap_dict[n]["section"] for n in gaps})
        selected_section = st.selectbox("Choose section to analyse", all_sections, index=0)
        gaps = [n for n in gaps if gap_dict[n]["section"] == selected_section]
        if not gaps:
            st.error("No gaps found for selected section.")
            st.stop()

    criterion_table, comments_by_gap, data_quality = compute_criterion_table(
        rows=rows,
        gaps=gaps,
        col_map=col_map,
        resp_id_col=resp_id_col,
        gap_dict=gap_dict
    )

    overall_stats = summarise(criterion_table)

    grouped = {}
    for r in criterion_table:
        grouped.setdefault(r["section"], []).append(r)
    section_stats = {sec: summarise(items) for sec, items in grouped.items()}

    comment_samples = pick_comment_samples(criterion_table, comments_by_gap, max_gaps=12, per_gap=6)

    st.subheader("Quick sanity check (computed from your data)")
    st.write(overall_stats)

    st.markdown("### Criterion table (means & gaps)")
    st.dataframe(criterion_table, use_container_width=True)

    prompt = build_claude_prompt(ctx, criterion_table, overall_stats, section_stats, comment_samples, data_quality)

    with st.spinner("🤖 Claude is thinking…"):
        result = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=MAX_CLAUDE_TOKENS,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}],
        ).content[0].text.strip()

    try:
        parsed = json.loads(result)
    except json.JSONDecodeError:
        st.error("Claude did not return valid JSON. Showing raw output below.")
        st.text_area("Raw output", result, height=300)
        st.stop()

    total = parsed.get("total_gap_overview", {})
    sections_out = parsed.get("sections", [])

    tabs = st.tabs(["Total overview", "By section", "Copy"])

    with tabs[0]:
        st.markdown("### Slide bullets")
        st.text_area("Bullets", "\n".join(total.get("slide_bullets", [])), height=180)
        st.markdown("### Narrative")
        st.text_area("Narrative", total.get("narrative", ""), height=240)

    with tabs[1]:
        if not sections_out:
            st.info("No section outputs returned.")
        else:
            names = [s.get("section", "Unnamed") for s in sections_out]
            pick = st.selectbox("Choose section output:", names, index=0)
            chosen = next((s for s in sections_out if s.get("section") == pick), sections_out[0])

            st.markdown("### Slide bullets")
            st.text_area("Bullets", "\n".join(chosen.get("slide_bullets", [])), height=180)
            st.markdown("### Narrative")
            st.text_area("Narrative", chosen.get("narrative", ""), height=240)

    with tabs[2]:
        st.markdown("### Total overview (copy)")
        st.text_area(
            "Total (copy)",
            "BULLETS:\n" + "\n".join(total.get("slide_bullets", [])) + "\n\nNARRATIVE:\n" + total.get("narrative", ""),
            height=260
        )
        st.markdown("### Sections (copy)")
        combined = []
        for s in sections_out:
            combined.append(f"== {s.get('section','Unnamed')} ==\n")
            combined.append("BULLETS:\n" + "\n".join(s.get("slide_bullets", [])) + "\n")
            combined.append("NARRATIVE:\n" + s.get("narrative", "") + "\n\n")
        st.text_area("Sections (copy)", "\n".join(combined), height=320)

# Sidebar context
st.sidebar.header("Project Context")
for k, v in ctx.items():
    if k.endswith("_raw"):
        st.sidebar.markdown(f"**{k.replace('_',' ').title()}:** (configured)")
    else:
        st.sidebar.markdown(f"**{k.replace('_',' ').title()}:** {v}")
