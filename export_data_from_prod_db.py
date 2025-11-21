# -------------------------ABOUT --------------------------

# pyinstaller --onefile --windowed export_data_from_prod_db.py
# Tool: Prod Database → SQLite Exporter Tool
# Developer: dyoliya
# Created: 2025-11-22

# © 2025 dyoliya. All rights reserved.

# ---------------------------------------------------------

"""

GUI tool to export specified MySQL (AWS RDS) tables to a local SQLite .db file.
Uses credentials from a .env file.

Behavior:
- Connects to MySQL using credentials in .env (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT).
- For each table in TABLES_TO_EXPORT, fetches rows in chunks and replaces the corresponding table
  in the local SQLite database (creates/overwrites).
- Progress bar shows overall progress (by rows).
"""

import os
import sys
import threading
import io
import math
import sqlite3
import mysql.connector
import json
from mysql.connector import Error
from dotenv import load_dotenv
import customtkinter as ctk
import tkinter as tk
from tkinter import messagebox, filedialog

# ---- CONFIG ----
# Tables to export (as requested)
TABLES_TO_EXPORT = [
    "contacts",
    "contact_skip_traced_addresses",
    "contact_addresses",
    "contact_phone_numbers",
    "contact_email_addresses",
    "contact_serial_numbers",
    "contact_targets",
    "remove_contacts",
    "upload_processors",
]

# Optional table-specific WHERE filters
TABLE_FILTERS = {
    "upload_processors": "uploaded_by IN (106, 109)",
    "remove_contacts": "removed_by_user_id IN (106, 109)"
}

# Default local DB file name
LOCAL_DB_NAME = "prod_database_local_copy.db"

# Chunk size for fetching/inserting rows
CHUNK_SIZE = 1000

# ---- END CONFIG ----

load_dotenv()  # load .env into environment

# read DB creds from environment
DB_HOST = os.getenv("MYSQL_HOST")
DB_USER = os.getenv("MYSQL_USER")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.getenv("MYSQL_DATABASE")
DB_PORT = int(os.getenv("MYSQL_PORT") or 3306)

# Helper: create db folder next to exe or script
base_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
database_folder = os.path.join(base_dir, "database")
os.makedirs(database_folder, exist_ok=True)
local_db_path = os.path.join(database_folder, LOCAL_DB_NAME)

# Load field selections
FIELDS_CONFIG_PATH = os.path.join(base_dir, "table_fields.json")

if os.path.exists(FIELDS_CONFIG_PATH):
    with open(FIELDS_CONFIG_PATH, "r") as f:
        TABLE_FIELDS = json.load(f)
else:
    TABLE_FIELDS = {}   # fallback if no JSON found

# ---------------- MySQL <> SQLite logic ----------------

def get_mysql_connection():
    """Return a MySQL connection using creds from .env. Raises on failure."""
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        raise RuntimeError("Missing database credentials. Ensure DB_HOST, DB_USER, DB_PASSWORD, DB_NAME are set in .env")
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        charset='utf8mb4',
        use_unicode=True,
        autocommit=False
    )
    return conn

def fetch_table_count(mysql_conn, table):
    cur = mysql_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    count = cur.fetchone()[0]
    cur.close()
    return count

def get_table_columns(mysql_conn, table):
    """
    Return the list of columns we should export for a table.
    Uses table_fields.json if provided.
    """
    cur = mysql_conn.cursor()
    cur.execute(f"SHOW COLUMNS FROM `{table}`")
    all_columns = [row[0] for row in cur.fetchall()]
    cur.close()

    # If JSON defines fields for this table → use them
    if table in TABLE_FIELDS:
        allowed = TABLE_FIELDS[table]
        # Only keep fields that exist in MySQL (avoid crash if mismatch)
        return [c for c in allowed if c in all_columns]

    # Otherwise → export ALL columns
    return all_columns

def recreate_sqlite_table(sqlite_conn, table, columns):
    """
    Create (or replace) a table in SQLite with the provided column names.
    All columns created as TEXT to avoid type mismatches (SQLite is dynamic-typed).
    """
    cur = sqlite_conn.cursor()
    cur.execute("PRAGMA foreign_keys = OFF;")
    sqlite_conn.commit()
    # safe name quoting
    col_defs = ", ".join([f"'{c}' TEXT" for c in columns])
    # drop and create to ensure replace/refresh behavior
    cur.execute(f"DROP TABLE IF EXISTS '{table}'")
    cur.execute(f"CREATE TABLE '{table}' ({col_defs});")
    sqlite_conn.commit()
    cur.close()

def export_table(mysql_conn, sqlite_conn, table, progress_callback=None, offset_progress=0.0, table_weight=1.0):
    """
    Export one table from MySQL to SQLite.
    progress_callback(fraction, message) -- fraction is 0..1 for the whole export run.
    offset_progress & table_weight let caller incorporate this table's progress into overall progress.
    """
    mysql_cur = mysql_conn.cursor(buffered=True)
    # get total rows
    try:
        total_rows = fetch_table_count(mysql_conn, table)
    except Exception:
        total_rows = 0

    columns = get_table_columns(mysql_conn, table)
    recreate_sqlite_table(sqlite_conn, table, columns)

    if total_rows == 0:
        # nothing to do but notify
        if progress_callback:
            progress_callback(offset_progress + table_weight, f"{table} (0 rows)")
        mysql_cur.close()
        return

    # prepare insert statement in sqlite
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO '{table}' ({', '.join(['`'+c+'`' for c in columns])}) VALUES ({placeholders})"

    # fetch in chunks
    # Optional WHERE condition
    where_clause = ""
    if table in TABLE_FILTERS:
        where_clause = f" WHERE {TABLE_FILTERS[table]}"

    select_sql = (
        f"SELECT {', '.join(['`'+c+'`' for c in columns])} "
        f"FROM `{table}`{where_clause}"
    )
    mysql_cur.execute(select_sql)
    sqlite_cur = sqlite_conn.cursor()

    rows_inserted = 0
    batch = []

    while True:
        rows = mysql_cur.fetchmany(CHUNK_SIZE)
        if not rows:
            break

        batch = []
        for r in rows:
            # convert bytes to str if needed
            converted = [v.decode('utf-8', errors='replace') if isinstance(v, (bytes, bytearray)) else v for v in r]
            batch.append(tuple(converted))

        sqlite_cur.executemany(insert_sql, batch)
        sqlite_conn.commit()

        # Update row-level progress
        for _ in batch:
            rows_inserted += 1
            if progress_callback and rows_inserted % 50 == 0:  # update every 50 rows
                table_progress = rows_inserted / total_rows
                overall_fraction = offset_progress + table_weight * table_progress
                progress_callback(overall_fraction, f"{table}: {rows_inserted}/{total_rows} rows ({table_progress*100:.1f}%)")


    mysql_cur.close()

def hide_sqlite_aux_files(db_path):
    """
    Hides the .db-wal and .db-shm files for the given SQLite database.
    Works on Windows (sets hidden attribute) and Unix (prefix with dot, already hidden).
    """
    wal_file = db_path + "-wal"
    shm_file = db_path + "-shm"

    # Windows
    if sys.platform == "win32":
        for f in [wal_file, shm_file]:
            if os.path.exists(f):
                # +h = hidden
                os.system(f'attrib +h "{f}"')
    else:
        pass

# ---------------- GUI ----------------

class RdsExportUI(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Prod Database → SQLite Exporter Tool v1.0.0")
        self.geometry("720x620")
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")
        self.configure(fg_color="#273946")

        # Title
        self.title_label = ctk.CTkLabel(self, text="Prod Database → SQLite Exporter", font=ctk.CTkFont(size=20, weight="bold"), text_color="#fff6de")
        self.title_label.pack(pady=(18, 6))

        # Buttons row
        btn_frame = ctk.CTkFrame(self, fg_color="#273946", corner_radius=0)
        btn_frame.pack(pady=6)

        self.choose_file_btn = ctk.CTkButton(btn_frame, text="Export Database To…", fg_color="#CB1F47", hover_color="#ffab4c", command=self.choose_output_file)
        self.choose_file_btn.pack(side="left", padx=6)

        self.refresh_btn = ctk.CTkButton(btn_frame, text="Refresh", fg_color="#CB1F47", hover_color="#ffab4c", command=self.refresh_status)
        self.refresh_btn.pack(side="left", padx=6)

        # File list / messages
        # Frame to hold Text + scrollbars
        text_frame = tk.Frame(self)
        text_frame.pack(padx=20, pady=(8, 6), fill="both", expand=False)

        # Vertical scrollbar
        v_scroll = tk.Scrollbar(text_frame, orient="vertical")
        v_scroll.pack(side="right", fill="y")

        # Horizontal scrollbar
        h_scroll = tk.Scrollbar(text_frame, orient="horizontal")
        h_scroll.pack(side="bottom", fill="x")

        # Text widget with scrollbars
        self.file_text = tk.Text(
            text_frame,
            height=10,
            bg="#fff6de",
            fg="#273946",
            font=("Segoe UI", 11),
            highlightthickness=0,
            relief="flat",
            wrap="none",  # important for horizontal scrollbar
            yscrollcommand=v_scroll.set,
            xscrollcommand=h_scroll.set
        )
        self.file_text.pack(side="left", fill="both", expand=True)

        # Configure scrollbars
        v_scroll.config(command=self.file_text.yview)
        h_scroll.config(command=self.file_text.xview)

        # Tag configuration remains
        self.file_text.tag_configure("header", font=("Segoe UI", 11, "bold"))
        self.file_text.configure(state="disabled")

        # Instruction
        self.instruction_label = ctk.CTkLabel(self, text="Exports selected .work database tables to a local .db file. Click START EXPORT.", text_color="#BBB8A6", wraplength=680, justify="center")
        self.instruction_label.pack(pady=6)

        # Message label
        self.message_label = ctk.CTkLabel(self, text="Waiting to start...", text_color="#BBB8A6")
        self.message_label.pack(pady=4)

        # Progress bar
        self.progress = ctk.CTkProgressBar(self, width=640, fg_color="#444444", progress_color="#CB1F47")
        self.progress.set(0)
        self.progress.pack(pady=10)

        # Start button
        self.run_btn = ctk.CTkButton(self, text="START EXPORT", width=160, height=44, corner_radius=8, fg_color="#CB1F47", hover_color="#ffab4c", font=ctk.CTkFont(size=14, weight="bold"), command=self.start_export)
        self.run_btn.pack(pady=10)

        # Table checkboxes (allow excluding some if desired)
        self.tables_frame = ctk.CTkFrame(self, fg_color="#273946", corner_radius=0)
        self.tables_frame.pack(padx=20, pady=(4,12), fill="x")
        self.table_vars = {}
        lbl = ctk.CTkLabel(self.tables_frame, text="Tables to export (toggle):", text_color="#BBB8A6")
        lbl.pack(anchor="w", pady=(4,2))
        # Table checkboxes in a grid (3 per row)
        grid_frame = ctk.CTkFrame(self.tables_frame, fg_color="#273946")
        grid_frame.pack(pady=4)

        cols_per_row = 3
        for index, t in enumerate(TABLES_TO_EXPORT):
            var = tk.BooleanVar(value=True)
            chk = ctk.CTkCheckBox(
                grid_frame, 
                text=t, 
                variable=var, 
                checkbox_width=18, 
                checkbox_height=18
            )
            row_index = index // cols_per_row
            col_index = index % cols_per_row
            chk.grid(row=row_index, column=col_index, sticky="w", padx=8, pady=4)

            self.table_vars[t] = var

        # Set default local db path display
        self.selected_db_path = local_db_path
        self.refresh_status()

        # github link (hihi)
        self.credit_label = ctk.CTkLabel(
        self,
        text="© dyoliya • GitHub",
        text_color="#737576",
        font=ctk.CTkFont(size=8, underline=False),
        cursor="hand2"
        )
        self.credit_label.place(relx=1.0, x=-10, y=1, anchor="ne") 
        self.credit_label.bind("<Button-1>", lambda e: self.open_url("https://github.com/dyoliya"))

        # Helper function
    def open_url(self, url):
        import webbrowser
        webbrowser.open(url)

    # UI actions
    def choose_output_file(self):
        path = filedialog.asksaveasfilename(title="Select output SQLite DB", defaultextension=".db", filetypes=[("SQLite DB", "*.db"), ("All files", "*.*")], initialfile=LOCAL_DB_NAME, initialdir=database_folder)
        if path:
            self.selected_db_path = path
            self.refresh_status()

    
    def refresh_status(self):
        self.progress.set(0)
        self.message_label.configure(text="Waiting to start...")
        self.file_text.configure(state="normal")
        self.file_text.delete("1.0", "end")
        self.file_text.insert("end", "Local DB file:\n", "header")
        if os.path.exists(self.selected_db_path):
            self.file_text.insert("end", f"  {self.selected_db_path}\n")
        else:
            self.file_text.insert("end", f"  (will be created) {self.selected_db_path}\n", "header")
        self.file_text.insert("end", "\nTables configured:\n", "header")
        for t, var in self.table_vars.items():
            self.file_text.insert("end", f"  {'[X]' if var.get() else '[ ]'} {t}\n")
        self.file_text.configure(state="disabled")
        # enable run if env creds look present
        self.run_btn.configure(state="normal" if all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]) else "disabled")
        if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
            self.instruction_label.configure(text="Missing DB creds in .env. Please set DB_HOST, DB_USER, DB_PASSWORD, DB_NAME.")
        else:
            self.instruction_label.configure(text="Ready. Click START EXPORT to sync the selected tables into the local .db file.")

    def open_folder(self, folder_path):
        if sys.platform == "win32":
            os.startfile(folder_path)
        elif sys.platform == "darwin":
            os.system(f'open "{folder_path}"')
        else:
            os.system(f'xdg-open "{folder_path}"')

    def start_export(self):
        # disable run button and start thread
        if not messagebox.askyesno("Start export", f"This will export selected tables to:\n\n{self.selected_db_path}\n\nContinue?"):
            return
        self.run_btn.configure(state="disabled")
        self.progress.set(0)
        self.message_label.configure(text="Starting export...")
        threading.Thread(target=self._export_worker, daemon=True).start()

    # progress_callback used by export functions
    def progress_callback(self, fraction, message=None):
        # fraction 0..1
        try:
            self.progress.set(fraction)
            if message:
                self.message_label.configure(text=message)
            # ensure GUI updates
            self.update_idletasks()
        except Exception:
            pass  # ignore GUI update issues

    def _export_worker(self):
        # gather selected tables
        selected_tables = [t for t, var in self.table_vars.items() if var.get()]
        if not selected_tables:
            messagebox.showinfo("No tables", "No tables selected for export.")
            self.run_btn.configure(state="normal")
            return

        # connect to mysql
        try:
            self.progress_callback(0.02, "Connecting to MySQL...")
            mysql_conn = get_mysql_connection()
        except Exception as e:
            messagebox.showerror("MySQL connection error", f"Failed to connect to MySQL:\n{e}")
            self.run_btn.configure(state="normal")
            self.progress_callback(0.0, "Connection failed.")
            return

        # open/create sqlite
        try:
            sqlite_conn = sqlite3.connect(self.selected_db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            sqlite_conn.execute("PRAGMA journal_mode=WAL;")
            sqlite_conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception as e:
            messagebox.showerror("SQLite error", f"Failed to open/create SQLite DB:\n{e}")
            mysql_conn.close()
            self.run_btn.configure(state="normal")
            return

        # compute weights for progress (equal weight per table)
        n = len(selected_tables)
        per_table_weight = 1.0 / n if n else 1.0

        try:
            tables_done = 0
            for t in selected_tables:
                offset = per_table_weight * tables_done
                self.progress_callback(offset, f"Preparing to export {t}...")
                export_table(mysql_conn, sqlite_conn, t, progress_callback=self.progress_callback, offset_progress=offset, table_weight=per_table_weight)
                tables_done += 1
            # final commit and close
            sqlite_conn.commit()
            sqlite_conn.close()
            mysql_conn.close()
            hide_sqlite_aux_files(self.selected_db_path) # hide WAL/SHM files
            self.progress_callback(1.0, f"Export complete. Local DB saved at:\n{self.selected_db_path}")
            
            def ask_open_folder():
                if messagebox.askyesno("Done", "Export finished!\nOpen output folder?"):
                    output_folder = os.path.dirname(self.selected_db_path)
                    if not os.path.exists(output_folder):
                        os.makedirs(output_folder)
                    self.open_folder(output_folder)  # reuse your open_folder method

            self.message_label.after(0, ask_open_folder)

        except Exception as e:
            try:
                sqlite_conn.close()
            except:
                pass
            try:
                mysql_conn.close()
            except:
                pass
            self.progress_callback(0.0, f"Export failed: {e}")
            messagebox.showerror("Export failed", f"An error occurred during export:\n{e}")
        finally:
            self.run_btn.configure(state="normal")
            self.refresh_status()

    
if __name__ == "__main__":
    app = RdsExportUI()
    app.mainloop()
