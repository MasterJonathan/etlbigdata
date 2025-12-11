import streamlit as st
import pandas as pd
import io
from sqlalchemy import create_engine, inspect

# --- KONFIGURASI HALAMAN ---

st.set_page_config(page_title="Proyek Big Data - ETL", layout="wide", page_icon="🚀")

# --- SESSION STATE (DATA STORE) ---
# Kita gunakan Dictionary untuk menyimpan BANYAK dataframe sekaligus
# Format: {'nama_file_atau_tabel': dataframe_objek}
if 'data_store' not in st.session_state:
    st.session_state.data_store = {}

if 'active_key' not in st.session_state:
    st.session_state.active_key = None

# --- SIDEBAR: DATA MANAGER ---
st.sidebar.title("🗄️ Data Manager")
st.sidebar.info("Data yang sudah di-load akan muncul di sini.")

# Dropdown untuk memilih Data Aktif yang mau di-edit
if st.session_state.data_store:
    key_options = list(st.session_state.data_store.keys())
    # Set default key jika belum ada
    if st.session_state.active_key not in key_options:
        st.session_state.active_key = key_options[0]
        
    selected_key = st.sidebar.selectbox("📂 Pilih Data Aktif:", key_options, index=key_options.index(st.session_state.active_key))
    st.session_state.active_key = selected_key
    
    # Tombol Hapus Data
    if st.sidebar.button("🗑️ Hapus Data Ini"):
        del st.session_state.data_store[selected_key]
        st.session_state.active_key = None
        st.rerun()
else:
    st.sidebar.warning("Belum ada data.")

menu = st.sidebar.radio("Tahapan ETL:", ["1. Extract (Multi Source)", "2. Transform (Olah)", "3. Load (Simpan)"])

# ==========================================
# 1. EXTRACT (MULTI SOURCE)
# ==========================================
if menu == "1. Extract (Multi Source)":
    st.header("1. Extract: Ambil Banyak Sumber Data")
    
    tab1, tab2, tab3 = st.tabs(["📁 Multi-Upload File", "🗄️ Database Tables", "➕ Union/Gabung Data"])
    
    # --- TAB 1: MULTI FILE UPLOAD ---
    with tab1:
        st.write("Upload banyak file sekaligus (Contoh: Data Jan, Data Feb, Data Mar).")
        uploaded_files = st.file_uploader("Upload File (CSV/Excel/Parquet)", type=['csv', 'xlsx', 'parquet'], accept_multiple_files=True)
        
        if uploaded_files:
            for uploaded_file in uploaded_files:
                # Cek agar tidak load ulang jika sudah ada
                if uploaded_file.name not in st.session_state.data_store:
                    try:
                        if uploaded_file.name.endswith('.csv'):
                            df = pd.read_csv(uploaded_file)
                        elif uploaded_file.name.endswith('.xlsx'):
                            df = pd.read_excel(uploaded_file)
                        elif uploaded_file.name.endswith('.parquet'):
                            df = pd.read_parquet(uploaded_file)
                        
                        st.session_state.data_store[uploaded_file.name] = df
                        st.toast(f"Berhasil load: {uploaded_file.name}")
                    except Exception as e:
                        st.error(f"Gagal load {uploaded_file.name}: {e}")
            
            # Refresh halaman agar sidebar update
            if st.button("Selesai Upload & Refresh"):
                st.rerun()

    # --- TAB 2: DATABASE MULTI TABLE ---
    with tab2:
        st.write("Koneksi ke Database dan pilih tabel-tabel yang diinginkan.")
        
        c1, c2 = st.columns(2)
        host = c1.text_input("Host", "localhost")
        user = c1.text_input("User", "root")
        password = c1.text_input("Password", type="password")
        dbname = c2.text_input("Database Name")
        
        # State khusus untuk list tabel DB
        if 'db_tables_list' not in st.session_state:
            st.session_state.db_tables_list = []

        if st.button("Connect & Scan Tables"):
            try:
                db_str = f'mysql+mysqlconnector://{user}:{password}@{host}/{dbname}'
                engine = create_engine(db_str)
                inspector = inspect(engine)
                tables = inspector.get_table_names()
                st.session_state.db_tables_list = tables
                st.success(f"Terhubung! Ditemukan {len(tables)} tabel.")
            except Exception as e:
                st.error(f"Koneksi Gagal: {e}")

        # Jika list tabel sudah ada, tampilkan multiselect
        if st.session_state.db_tables_list:
            selected_tables = st.multiselect("Pilih Tabel untuk di-load:", st.session_state.db_tables_list)
            
            if st.button("Load Tabel Terpilih"):
                db_str = f'mysql+mysqlconnector://{user}:{password}@{host}/{dbname}'
                engine = create_engine(db_str)
                
                for tbl in selected_tables:
                    if tbl not in st.session_state.data_store:
                        df = pd.read_sql(tbl, engine)
                        st.session_state.data_store[tbl] = df
                        st.toast(f"Tabel {tbl} berhasil di-load!")
                st.rerun()

    # --- TAB 3: UNION (GABUNG DATA) ---
    with tab3:
        st.subheader("Gabung Beberapa Data (Union)")
        st.info("Berguna jika Anda punya data terpisah (Jan, Feb, Mar) dengan kolom yang sama.")
        
        available_dfs = list(st.session_state.data_store.keys())
        union_candidates = st.multiselect("Pilih Data untuk Digabung:", available_dfs)
        new_name = st.text_input("Nama Data Gabungan Baru", "Data_Gabungan_All")
        
        if st.button("Proses Union"):
            if len(union_candidates) < 2:
                st.error("Pilih minimal 2 data.")
            else:
                try:
                    dfs_to_merge = [st.session_state.data_store[k] for k in union_candidates]
                    merged_df = pd.concat(dfs_to_merge, ignore_index=True)
                    st.session_state.data_store[new_name] = merged_df
                    st.success(f"Berhasil menggabungkan data! Total baris: {len(merged_df)}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Gagal gabung: {e}")

# ==========================================
# 2. TRANSFORM (PADA DATA AKTIF)
# ==========================================
elif menu == "2. Transform (Olah)":
    active_k = st.session_state.active_key
    
    if not active_k:
        st.warning("⚠️ Belum ada data aktif dipilih di Sidebar.")
    else:
        st.header(f"2. Transform: Mengedit '{active_k}'")
        df = st.session_state.data_store[active_k]
        
        # PREVIEW
        st.dataframe(df.head(5))
        st.caption(f"Rows: {df.shape[0]} | Cols: {df.shape[1]}")
        
        # --- TAB TRANSFORM ---
        t1, t2, t3 = st.tabs(["Cleaning & Filter", "Column Ops", "Join Tables"])
        
        with t1: # CLEANING
            col1, col2 = st.columns(2)
            if col1.button("Fill NA (Unknown/0)"):
                num = df.select_dtypes(include=['number']).columns
                obj = df.select_dtypes(include=['object']).columns
                df[num] = df[num].fillna(0)
                df[obj] = df[obj].fillna("Unknown")
                st.session_state.data_store[active_k] = df
                st.rerun()
                
            if col2.button("Remove Duplicates"):
                df = df.drop_duplicates()
                st.session_state.data_store[active_k] = df
                st.rerun()
                
            # Filter
            f_col = st.selectbox("Filter Kolom:", df.columns)
            f_val = st.text_input("Nilai Filter:")
            if st.button("Apply Filter"):
                df = df[df[f_col].astype(str).str.contains(f_val, case=False, na=False)]
                st.session_state.data_store[active_k] = df
                st.rerun()

        with t2: # COLUMN OPS
            split_col = st.selectbox("Split Kolom:", df.columns)
            if st.button("Split by Spasi"):
                new = df[split_col].str.split(" ", expand=True).add_prefix(f"{split_col}_")
                df = pd.concat([df, new], axis=1)
                st.session_state.data_store[active_k] = df
                st.rerun()

        with t3: # JOIN
            st.write(f"Join **{active_k}** dengan tabel lain:")
            other_keys = [k for k in st.session_state.data_store.keys() if k != active_k]
            
            if not other_keys:
                st.warning("Butuh minimal 2 data terload untuk melakukan Join.")
            else:
                right_table_name = st.selectbox("Pilih Tabel Pasangan (Right):", other_keys)
                left_on = st.selectbox("Kunci Tabel Kiri:", df.columns)
                right_df = st.session_state.data_store[right_table_name]
                right_on = st.selectbox("Kunci Tabel Kanan:", right_df.columns)
                
                if st.button("Lakukan Join"):
                    merged = pd.merge(df, right_df, left_on=left_on, right_on=right_on, how='left')
                    # Simpan sebagai data baru
                    new_name = f"Join_{active_k}_vs_{right_table_name}"
                    st.session_state.data_store[new_name] = merged
                    st.success(f"Join sukses! Disimpan sebagai '{new_name}'")
                    st.rerun()

# ==========================================
# 3. LOAD (SIMPAN)
# ==========================================
elif menu == "3. Load (Simpan)":
    active_k = st.session_state.active_key
    
    if not active_k:
        st.warning("Pilih data di sidebar dulu.")
    else:
        st.header(f"3. Load: Simpan '{active_k}'")
        df = st.session_state.data_store[active_k]
        
        st.dataframe(df.head())
        
        target = st.selectbox("Target Simpan:", ["Hadoop (Parquet)", "MySQL Database"])
        
        if target == "Hadoop (Parquet)":
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            st.download_button("Download Parquet", buffer.getvalue(), "hasil.parquet")
            
        elif target == "MySQL Database":
            c1, c2 = st.columns(2)
            h = c1.text_input("Host", "localhost", key="lh")
            u = c1.text_input("User", "root", key="lu")
            p = c1.text_input("Pass", type="password", key="lp")
            d = c2.text_input("DB Name", key="ld")
            t = c2.text_input("Table Name", key="lt")
            
            if st.button("Push to DB"):
                try:
                    eng = create_engine(f'mysql+mysqlconnector://{u}:{p}@{h}/{d}')
                    df.to_sql(t, eng, if_exists='replace', index=False)
                    st.success("Tersimpan di Database!")
                except Exception as e:
                    st.error(f"Error: {e}")