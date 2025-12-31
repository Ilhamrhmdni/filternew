# app.py
import os
import re
import csv
import uuid
import shutil
import tempfile

import numpy as np
import pandas as pd
import streamlit as st

from paste_streamer import paste_streamer

st.set_page_config(page_title="Shopee Product Filter", layout="wide")

COLUMNS_10 = [
    "No", "Link Produk", "Nama Produk", "Harga", "Stock",
    "Terjual Bulanan", "Terjual Semua", "Komisi %", "Komisi Rp", "Ratting"
]
NUM_COLS = ["No", "Harga", "Stock", "Terjual Bulanan", "Terjual Semua", "Komisi %", "Komisi Rp", "Ratting"]

REJ_LOG_COLS_CSV = COLUMNS_10 + ["Sumber", "Baris Ke", "Kolom Error", "Alasan"]
REJ_LOG_COLS_TXT = [
    "Link Produk", "Nama Produk", "Harga", "Stock", "Terjual Bulanan", "Terjual Semua",
    "Komisi %", "Komisi Rp", "Ratting",
    "Sumber", "Baris Ke", "Kolom Error", "Alasan"
]

# =========================
# SESSION STATE INIT
# =========================
if "stage" not in st.session_state:
    st.session_state.stage = "input"  # input -> ready -> filtered

if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())[:8]

if "workdir" not in st.session_state:
    st.session_state.workdir = tempfile.mkdtemp(prefix=f"shopee_{st.session_state.session_id}_")

if "paths" not in st.session_state:
    wd = st.session_state.workdir
    st.session_state.paths = {
        "raw_input": os.path.join(wd, "raw_input.txt"),
        "cleaned_tsv": os.path.join(wd, "cleaned.tsv"),
        "passed_csv": os.path.join(wd, "passed.csv"),
        "passed_txt": os.path.join(wd, "passed.txt"),
        "reject_csv": os.path.join(wd, "reject_log.csv"),
        "reject_txt": os.path.join(wd, "reject_log.txt"),
    }

if "prep_stats" not in st.session_state:
    st.session_state.prep_stats = {}

if "filter_stats" not in st.session_state:
    st.session_state.filter_stats = {}

# paste-streaming receiver state
if "ps_job_id" not in st.session_state:
    st.session_state.ps_job_id = ""
if "ps_ack_seq" not in st.session_state:
    st.session_state.ps_ack_seq = -1
if "ps_total_chars" not in st.session_state:
    st.session_state.ps_total_chars = 0
if "ps_done" not in st.session_state:
    st.session_state.ps_done = False
if "ps_received_chars" not in st.session_state:
    st.session_state.ps_received_chars = 0


# =========================
# HELPERS
# =========================
def detect_sep_from_sample(sample: str) -> str:
    return "\t" if "\t" in (sample or "") else ","

def clean_number_id(x):
    if x is None:
        return np.nan
    s = str(x).strip()
    if s == "" or s.lower() in ["nan", "none", "-", "null"]:
        return np.nan
    s = s.replace("Rp", "").replace("rp", "").strip()
    s = s.replace(" ", "")
    s = re.sub(r"[^0-9\.,]", "", s)
    s = s.replace(".", "").replace(",", "")
    return pd.to_numeric(s, errors="coerce")

def parse_terms(raw: str):
    parts = re.split(r"[,\n;]+", (raw or ""))
    return [p.strip() for p in parts if p.strip()]

def fmt_id(x):
    try:
        if pd.isna(x):
            return "-"
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return str(x)

def safe_filesize_mb(path: str) -> float:
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except Exception:
        return 0.0

def write_reject_header(csv_path: str):
    need_header = (not os.path.exists(csv_path)) or (os.path.getsize(csv_path) == 0)
    if need_header:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(REJ_LOG_COLS_CSV)

def init_cleaned_file(cleaned_path: str):
    with open(cleaned_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(COLUMNS_10)

def append_cleaned_rows(cleaned_path: str, df_valid: pd.DataFrame):
    if df_valid is None or df_valid.empty:
        return
    with open(cleaned_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        for _, r in df_valid.iterrows():
            out = []
            for c in COLUMNS_10:
                v = r.get(c, "")
                if pd.isna(v):
                    out.append("")
                else:
                    out.append(str(int(v)) if c in NUM_COLS else str(v))
            w.writerow(out)

def process_streaming_prepare(raw_path: str, cleaned_path: str, reject_csv: str, reject_txt: str,
                             sep: str, chunk_size: int = 100_000):
    init_cleaned_file(cleaned_path)

    total_lines = 0
    good_rows_total = 0
    bad_parse = 0
    bad_clean = 0

    auto_no = 1
    buffer_rows = []
    buffer_meta = []

    def append_reject_csv_rows(rows: list[dict]):
        if not rows:
            return
        write_reject_header(reject_csv)
        with open(reject_csv, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for r in rows:
                w.writerow([r.get(c, "") for c in REJ_LOG_COLS_CSV])

        with open(reject_txt, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter="\t")
            for r in rows:
                w.writerow([r.get(c, "") for c in REJ_LOG_COLS_TXT])

    def flush_buffer():
        nonlocal good_rows_total, bad_clean, buffer_rows, buffer_meta
        if not buffer_rows:
            return

        df = pd.DataFrame(buffer_rows, columns=COLUMNS_10)
        linenos = buffer_meta

        df["Link Produk"] = df["Link Produk"].astype(str).str.strip()
        df["Nama Produk"] = df["Nama Produk"].astype(str).str.strip()

        bad_cols_map: dict[int, list[str]] = {}

        for col in NUM_COLS:
            orig = df[col].fillna("").astype(str).str.strip()
            empty_mask = orig.eq("") | orig.str.lower().isin(["nan", "none", "-", "null"])
            cleaned = orig.map(clean_number_id)

            invalid_mask = (~empty_mask) & cleaned.isna()
            if invalid_mask.any():
                for i in df.index[invalid_mask].tolist():
                    bad_cols_map.setdefault(i, []).append(col)

            df[col] = cleaned

        if bad_cols_map:
            bad_idxs = sorted(bad_cols_map.keys())
            bad_clean += len(bad_idxs)

            bad_rows = []
            for i in bad_idxs:
                r = df.loc[i]
                bad_rows.append({
                    **{c: ("" if pd.isna(r.get(c)) else r.get(c)) for c in COLUMNS_10},
                    "Sumber": "CLEAN",
                    "Baris Ke": linenos[i],
                    "Kolom Error": ", ".join(bad_cols_map[i]),
                    "Alasan": "Nilai angka tidak valid pada kolom tersebut.",
                })
            append_reject_csv_rows(bad_rows)
            df = df.drop(index=bad_idxs)

        if df.empty:
            buffer_rows.clear()
            buffer_meta.clear()
            return

        for col in NUM_COLS:
            df[col] = df[col].round(0).astype("Int64")

        append_cleaned_rows(cleaned_path, df)
        good_rows_total += len(df)

        buffer_rows.clear()
        buffer_meta.clear()

    with open(raw_path, "r", encoding="utf-8", errors="ignore") as f:
        for lineno, line in enumerate(f, start=1):
            total_lines += 1
            raw_line = line.strip("\n")
            if not raw_line.strip():
                continue

            try:
                row = next(csv.reader([raw_line], delimiter=sep))
            except Exception as e:
                bad_parse += 1
                append_reject_csv_rows([{
                    **{c: "" for c in COLUMNS_10},
                    "Sumber": "PARSE",
                    "Baris Ke": lineno,
                    "Kolom Error": "Parsing",
                    "Alasan": f"Gagal parsing: {e}",
                }])
                continue

            if len(row) == 10:
                buffer_rows.append(row); buffer_meta.append(lineno)
            elif len(row) == 9:
                buffer_rows.append([str(auto_no)] + row); buffer_meta.append(lineno); auto_no += 1
            else:
                bad_parse += 1
                append_reject_csv_rows([{
                    **{c: "" for c in COLUMNS_10},
                    "Sumber": "PARSE",
                    "Baris Ke": lineno,
                    "Kolom Error": "Jumlah Kolom",
                    "Alasan": f"Jumlah kolom {len(row)} tidak sesuai (harus 9 atau 10).",
                }])
                continue

            if len(buffer_rows) >= chunk_size:
                flush_buffer()

    flush_buffer()

    return {
        "total_lines": total_lines,
        "valid_rows": good_rows_total,
        "bad_parse": bad_parse,
        "bad_clean": bad_clean,
    }

def run_streaming_filter(cleaned_path: str, passed_csv: str, passed_txt: str,
                         reject_csv: str, reject_txt: str,
                         min_stock: float, min_tb: float, min_kpct: float, min_krp: float,
                         include_terms: list[str], chunk_size: int = 200_000):
    with open(passed_csv, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(COLUMNS_10)
    with open(passed_txt, "w", newline="", encoding="utf-8"):
        pass

    pattern = None
    if include_terms:
        pattern = "|".join(re.escape(t) for t in include_terms)

    total_seen = 0
    total_passed = 0
    total_rejected_filter = 0
    sum_tb = 0
    sum_krp = 0

    prog = st.progress(0)
    status = st.empty()
    approx_total = st.session_state.prep_stats.get("valid_rows", 0) or 0

    def to_int0(series):
        return pd.to_numeric(series, errors="coerce").fillna(0).astype(np.int64)

    def add_field(current: pd.Series, cond: pd.Series, label: str, sep: str):
        cur = current.to_numpy()
        c = cond.to_numpy()
        add = np.where(c, label, "")
        out = np.where(cur == "", add, np.where(add == "", cur, cur + sep + add))
        return pd.Series(out, index=current.index, dtype=object)

    for chunk in pd.read_csv(cleaned_path, sep="\t", chunksize=chunk_size, dtype=str):
        total_seen += len(chunk)

        stock = to_int0(chunk["Stock"])
        tb = to_int0(chunk["Terjual Bulanan"])
        kpct = to_int0(chunk["Komisi %"])
        krp = to_int0(chunk["Komisi Rp"])

        mask = (stock >= int(min_stock)) & (tb >= int(min_tb)) & (kpct >= int(min_kpct)) & (krp >= int(min_krp))

        name_ok = None
        if pattern:
            name_ok = chunk["Nama Produk"].fillna("").astype(str).str.contains(pattern, case=False, regex=True, na=False)
            mask = mask & name_ok

        passed = chunk[mask].copy()
        rejected = chunk[~mask].copy()

        if not passed.empty:
            sum_tb += int(pd.to_numeric(passed["Terjual Bulanan"], errors="coerce").fillna(0).sum())
            sum_krp += int(pd.to_numeric(passed["Komisi Rp"], errors="coerce").fillna(0).sum())
            total_passed += len(passed)

            passed.to_csv(passed_csv, mode="a", header=False, index=False, columns=COLUMNS_10)

            cols_txt = ["Link Produk","Nama Produk","Harga","Stock","Terjual Bulanan","Terjual Semua","Komisi %","Komisi Rp","Ratting"]
            passed[cols_txt].to_csv(passed_txt, mode="a", header=False, index=False, sep="\t")

        if not rejected.empty:
            total_rejected_filter += len(rejected)
            idx = rejected.index

            stock_f = stock.loc[idx] < int(min_stock)
            tb_f = tb.loc[idx] < int(min_tb)
            kpct_f = kpct.loc[idx] < int(min_kpct)
            krp_f = krp.loc[idx] < int(min_krp)
            if pattern:
                name_f = ~name_ok.loc[idx]
            else:
                name_f = pd.Series(False, index=idx)

            kol = pd.Series("", index=idx, dtype=object)
            kol = add_field(kol, stock_f, "Stock", ", ")
            kol = add_field(kol, tb_f, "Terjual Bulanan", ", ")
            kol = add_field(kol, kpct_f, "Komisi %", ", ")
            kol = add_field(kol, krp_f, "Komisi Rp", ", ")
            if pattern:
                kol = add_field(kol, name_f, "Nama Produk", ", ")

            alasan = pd.Series("", index=idx, dtype=object)
            alasan = add_field(alasan, stock_f, f"Stock < {int(min_stock)}", " | ")
            alasan = add_field(alasan, tb_f, f"Terjual Bulanan < {int(min_tb)}", " | ")
            alasan = add_field(alasan, kpct_f, f"Komisi % < {int(min_kpct)}", " | ")
            alasan = add_field(alasan, krp_f, f"Komisi Rp < {int(min_krp)}", " | ")
            if pattern and include_terms:
                alasan = add_field(alasan, name_f, f"Nama tidak mengandung: {', '.join(include_terms)}", " | ")

            rej_df = rejected[COLUMNS_10].copy()
            rej_df["Sumber"] = "FILTER"
            rej_df["Baris Ke"] = ""
            rej_df["Kolom Error"] = kol
            rej_df["Alasan"] = np.where(alasan.to_numpy() == "", "Tidak lolos filter", alasan.to_numpy())

            write_reject_header(reject_csv)
            rej_df.to_csv(reject_csv, mode="a", header=False, index=False, columns=REJ_LOG_COLS_CSV)

            rej_txt_df = rej_df.reindex(columns=REJ_LOG_COLS_TXT)
            rej_txt_df.to_csv(reject_txt, mode="a", header=False, index=False, sep="\t")

        if approx_total > 0:
            prog.progress(min(1.0, total_seen / approx_total))
        status.write("Processing...")

    prog.progress(1.0)
    status.empty()

    return {
        "seen_valid_rows": total_seen,
        "passed_rows": total_passed,
        "rejected_filter_rows": total_rejected_filter,
        "sum_terjual_bulanan_passed": sum_tb,
        "sum_komisi_rp_passed": sum_krp,
    }

# =========================
# UI HEADER
# =========================
st.markdown("## Shopee Product Filter")
st.caption("Input tanpa header • Proses streaming • Export CSV/TXT + Log")

paths = st.session_state.paths

# =========================
# SIDEBAR (input + filter)
# =========================
with st.sidebar:
    st.header("Input Data")
    source_mode = st.radio("Sumber data", ["Paste (Streaming)", "Upload (.txt / .csv)"], index=0)

    uploaded = None

    if source_mode.startswith("Paste"):
        payload = paste_streamer(
            height=230,
            chunk_chars=2_000_000,
            ack_seq=st.session_state.ps_ack_seq,
            key="paste_streamer",
        )

        # Receiver: simpan raw paste ke file (tanpa parsing)
        if isinstance(payload, dict) and payload.get("type") == "start":
            st.session_state.ps_job_id = payload.get("job_id", "")
            st.session_state.ps_ack_seq = -1
            st.session_state.ps_total_chars = int(payload.get("total_chars", 0))
            st.session_state.ps_received_chars = 0
            st.session_state.ps_done = False
            # truncate raw file
            with open(paths["raw_input"], "w", encoding="utf-8", newline="") as f:
                f.write("")

        elif isinstance(payload, dict) and payload.get("type") == "chunk":
            job_id = payload.get("job_id", "")
            seq = int(payload.get("seq", -1))
            data = payload.get("data", "")

            if job_id and job_id == st.session_state.ps_job_id:
                # terima hanya urutan yang benar (ACK handshake)
                if seq == st.session_state.ps_ack_seq + 1:
                    with open(paths["raw_input"], "a", encoding="utf-8", newline="") as f:
                        f.write(data)
                    st.session_state.ps_ack_seq = seq
                    st.session_state.ps_received_chars += len(data)

        elif isinstance(payload, dict) and payload.get("type") == "done":
            job_id = payload.get("job_id", "")
            if job_id and job_id == st.session_state.ps_job_id:
                st.session_state.ps_done = True

    else:
        uploaded = st.file_uploader("Upload file (tanpa header)", type=["txt", "csv"])

    st.divider()
    c1, c2 = st.columns(2)
    start_btn = c1.button("▶️ MULAI PROSES", use_container_width=True)
    reset_btn = c2.button("🔄 RESET", use_container_width=True)

    st.divider()
    st.header("Filter")
    with st.form("filter_form"):
        min_stock = st.number_input("Stock minimal", min_value=0, value=0, step=1)
        min_tb = st.number_input("Terjual Bulanan minimal", min_value=0, value=0, step=1)
        min_kpct = st.number_input("Komisi % minimal", min_value=0.0, value=0.0, step=1.0, format="%.2f")
        min_krp = st.number_input("Komisi Rp minimal", min_value=0.0, value=0.0, step=100.0, format="%.0f")
        include_words_raw = st.text_input("Nama Produk wajib mengandung", value="", placeholder="contoh: anak, bayi, kids")
        run_filter_btn = st.form_submit_button("🚀 JALANKAN FILTER")

# =========================
# RESET
# =========================
if reset_btn:
    try:
        shutil.rmtree(st.session_state.workdir, ignore_errors=True)
    except Exception:
        pass
    for k in list(st.session_state.keys()):
        if k.startswith("ps_") or k in ["stage", "prep_stats", "filter_stats", "paths", "workdir", "session_id"]:
            del st.session_state[k]
    st.rerun()

# =========================
# STAGE INPUT: tombol proses
# =========================
if st.session_state.stage == "input":
    if start_btn:
        # siapkan raw_input dari upload jika mode upload
        if source_mode.startswith("Upload"):
            if uploaded is None:
                st.error("Belum upload file.")
                st.stop()
            data = uploaded.getvalue()
            try:
                txt = data.decode("utf-8")
            except UnicodeDecodeError:
                txt = data.decode("latin-1", errors="ignore")
            with open(paths["raw_input"], "w", encoding="utf-8", newline="") as f:
                f.write(txt)

        else:
            # mode paste streaming: pastikan paste sudah selesai
            if not st.session_state.ps_done:
                st.error("Paste belum selesai. Tunggu sampai selesai dulu, lalu klik MULAI PROSES.")
                st.stop()
            if not os.path.exists(paths["raw_input"]) or os.path.getsize(paths["raw_input"]) == 0:
                st.error("Belum ada data yang diterima dari paste.")
                st.stop()

        # reset output files
        for p in [paths["cleaned_tsv"], paths["passed_csv"], paths["passed_txt"], paths["reject_csv"], paths["reject_txt"]]:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

        # deteksi separator dari baris pertama raw_input (tanpa parsing tabel dulu)
        with open(paths["raw_input"], "r", encoding="utf-8", errors="ignore") as f:
            first_line = f.readline()
        sep = detect_sep_from_sample(first_line)

        with st.spinner("Processing..."):
            stats = process_streaming_prepare(
                raw_path=paths["raw_input"],
                cleaned_path=paths["cleaned_tsv"],
                reject_csv=paths["reject_csv"],
                reject_txt=paths["reject_txt"],
                sep=sep,
                chunk_size=100_000,
            )

        st.session_state.prep_stats = stats
        st.session_state.filter_stats = {}
        st.session_state.stage = "ready"
        st.rerun()

    st.stop()

# =========================
# MAIN TABS
# =========================
tab_rincian, tab_export = st.tabs(["Rincian", "Export"])

prep = st.session_state.prep_stats or {}
fs = st.session_state.filter_stats or {}

with tab_rincian:
    st.subheader("Rincian Data")

    cA, cB, cC, cD = st.columns(4)
    cA.metric("Total baris (raw)", f"{prep.get('total_lines', 0):,}".replace(",", "."))
    cB.metric("Baris valid", f"{prep.get('valid_rows', 0):,}".replace(",", "."))
    cC.metric("Skip (PARSE)", f"{prep.get('bad_parse', 0):,}".replace(",", "."))
    cD.metric("Skip (CLEAN)", f"{prep.get('bad_clean', 0):,}".replace(",", "."))

    s1, s2, s3 = st.columns(3)
    s1.metric("Cleaned file (TSV) MB", f"{safe_filesize_mb(paths['cleaned_tsv']):.1f}")
    s2.metric("Log (CSV) MB", f"{safe_filesize_mb(paths['reject_csv']):.1f}")
    s3.metric("Log (TXT) MB", f"{safe_filesize_mb(paths['reject_txt']):.1f}")

    if run_filter_btn:
        terms = parse_terms(include_words_raw)
        with st.spinner("Filtering..."):
            stats = run_streaming_filter(
                cleaned_path=paths["cleaned_tsv"],
                passed_csv=paths["passed_csv"],
                passed_txt=paths["passed_txt"],
                reject_csv=paths["reject_csv"],
                reject_txt=paths["reject_txt"],
                min_stock=min_stock,
                min_tb=min_tb,
                min_kpct=min_kpct,
                min_krp=min_krp,
                include_terms=terms,
                chunk_size=200_000,
            )
        st.session_state.filter_stats = stats
        st.session_state.stage = "filtered"
        st.rerun()

    if st.session_state.stage == "filtered":
        st.divider()
        st.subheader("Rincian Hasil Filter")

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Valid diproses", f"{fs.get('seen_valid_rows', 0):,}".replace(",", "."))
        m2.metric("Lolos", f"{fs.get('passed_rows', 0):,}".replace(",", "."))
        m3.metric("Total Terjual Bulanan (lolos)", fmt_id(fs.get("sum_terjual_bulanan_passed", 0)))
        m4.metric("Total Komisi Rp (lolos)", fmt_id(fs.get("sum_komisi_rp_passed", 0)))

with tab_export:
    st.subheader("Export")

    if st.session_state.stage != "filtered":
        st.info("Jalankan filter terlebih dulu untuk mengaktifkan export.")
    else:
        def sanitize_basename(name: str, fallback: str) -> str:
            name = (name or "").strip()
            if not name:
                name = fallback
            name = re.sub(r"\.(csv|txt)\s*$", "", name, flags=re.IGNORECASE)
            name = re.sub(r'[\\/*?:"<>|]+', "_", name)
            name = name.strip().strip(".")
            return name or fallback

        base_name = sanitize_basename(st.text_input("Nama file hasil", value="hasil_filter_produk"), "hasil_filter_produk")
        log_name = sanitize_basename(st.text_input("Nama file log", value=f"{base_name}_log_tidak_lolos"),
                                     f"{base_name}_log_tidak_lolos")

        col1, col2 = st.columns(2)

        with col1:
            fmt = st.radio("Format hasil", ["CSV", "TXT"], horizontal=True, index=0, key="fmt_pass")
            out_path = paths["passed_csv"] if fmt == "CSV" else paths["passed_txt"]
            out_file = f"{base_name}.csv" if fmt == "CSV" else f"{base_name}.txt"
            mime = "text/csv" if fmt == "CSV" else "text/plain"
            if os.path.exists(out_path):
                with open(out_path, "rb") as f:
                    st.download_button("Download hasil", data=f.read(), file_name=out_file, mime=mime)

        with col2:
            fmt2 = st.radio("Format log", ["CSV", "TXT"], horizontal=True, index=0, key="fmt_log")
            log_path = paths["reject_csv"] if fmt2 == "CSV" else paths["reject_txt"]
            log_file = f"{log_name}.csv" if fmt2 == "CSV" else f"{log_name}.txt"
            log_mime = "text/csv" if fmt2 == "CSV" else "text/plain"
            if os.path.exists(log_path):
                with open(log_path, "rb") as f:
                    st.download_button("Download log", data=f.read(), file_name=log_file, mime=log_mime)
