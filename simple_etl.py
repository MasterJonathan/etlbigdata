import streamlit as st
import pandas as pd
import io
from sqlalchemy import create_engine, inspect

# --- KONFIGURASI HALAMAN ---

st.set_page_config(page_title="Proyek Big Data - ETL", layout="wide", page_icon="🚀")
st.title("abcd")

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
        st.warning("⚠️ Belum ada data aktif dipilih di Sidebar. Silakan pilih data dulu.")
    else:
        st.header(f"2. Transform: Mengedit '{active_k}'")
        df = st.session_state.data_store[active_k]
        
        # --- PREVIEW DATA ---
        with st.expander("🔍 Lihat Data Saat Ini", expanded=True):
            st.dataframe(df.head(5))
            st.caption(f"Total Baris: {df.shape[0]} | Total Kolom: {df.shape[1]}")
        
        # --- MENU TRANSFORMASI ---
        # Membagi fitur ke dalam 4 Tab sesuai permintaan
        t1, t2, t3, t4 = st.tabs([
            "🧹 Cleaning", 
            "🔧 Manipulation", 
            "📝 Column Ops", 
            "🔗 Relational (Join)"
        ])
        
        # --- TAB 1: DATA CLEANING ---
        with t1:
            st.subheader("Data Cleaning")
            col_c1, col_c2 = st.columns(2)
            
            with col_c1:
                st.markdown("**1. Fill The Blank**")
                fill_val = st.text_input("Isi data teks kosong dengan:", "Unknown")
                if st.button("Isi Data Kosong"):
                    # Logika: Angka diisi 0, Teks diisi input user
                    num_cols = df.select_dtypes(include=['number']).columns
                    obj_cols = df.select_dtypes(include=['object']).columns
                    
                    df[num_cols] = df[num_cols].fillna(0)
                    df[obj_cols] = df[obj_cols].fillna(fill_val)
                    
                    st.session_state.data_store[active_k] = df
                    st.success("Data kosong berhasil diisi.")
                    st.rerun()
            
            with col_c2:
                st.markdown("**2. Remove Duplicates**")
                if st.button("Hapus Duplikat"):
                    before = len(df)
                    df = df.drop_duplicates()
                    st.session_state.data_store[active_k] = df
                    st.success(f"Berhasil menghapus {before - len(df)} baris duplikat.")
                    st.rerun()

        # --- TAB 2: DATA MANIPULATION ---
        with t2:
            st.subheader("Data Manipulation")
            
            # A. REPLACE
            with st.expander("A. Replace Value (Ganti Nilai)"):
                c_rep1, c_rep2, c_rep3 = st.columns(3)
                rep_col = c_rep1.selectbox("Pilih Kolom:", df.columns, key="rep_col")
                old_val = c_rep2.text_input("Nilai Lama")
                new_val = c_rep3.text_input("Nilai Baru")
                
                if st.button("Ganti Nilai"):
                    # Coba konversi ke angka jika memungkinkan agar tidak error tipe data
                    if df[rep_col].dtype != 'object':
                        try: old_val = float(old_val)
                        except: pass
                        try: new_val = float(new_val)
                        except: pass
                    
                    df[rep_col] = df[rep_col].replace(old_val, new_val)
                    st.session_state.data_store[active_k] = df
                    st.success(f"Mengganti '{old_val}' menjadi '{new_val}'")
                    st.rerun()

            # B. FILTER
            with st.expander("B. Filter Data (Saring)"):
                c_fil1, c_fil2 = st.columns(2)
                fil_col = c_fil1.selectbox("Filter Berdasarkan Kolom:", df.columns, key="fil_col")
                fil_val = c_fil2.text_input("Nilai yang dicari (Contains):")
                
                if st.button("Terapkan Filter"):
                    # Filter string contains (case insensitive)
                    df = df[df[fil_col].astype(str).str.contains(fil_val, case=False, na=False)]
                    st.session_state.data_store[active_k] = df
                    st.success(f"Filter diterapkan. Sisa baris: {len(df)}")
                    st.rerun()

            # C. TRANSPOSE
            with st.expander("C. Transpose (Putar Baris <> Kolom)"):
                st.warning("⚠️ Transpose akan mengubah struktur data secara total.")
                if st.button("Lakukan Transpose"):
                    df = df.T.reset_index()
                    st.session_state.data_store[active_k] = df
                    st.success("Transpose berhasil.")
                    st.rerun()

        # --- TAB 3: COLUMN OPERATIONS ---
        with t3:
            st.subheader("Column Operations")
            
            # A. SPLIT COLUMN
            with st.expander("A. Split Column (Pecah Kolom)"):
                split_col = st.selectbox("Pilih Kolom untuk Dipecah:", df.columns, key="split_col")
                delimiter = st.text_input("Pemisah (Delimiter)", " ", help="Contoh: koma (,), spasi ( ), strip (-)")
                
                if st.button("Pecah Kolom"):
                    try:
                        new_cols = df[split_col].str.split(delimiter, expand=True)
                        # Beri nama kolom otomatis
                        new_cols.columns = [f"{split_col}_{i+1}" for i in range(new_cols.shape[1])]
                        df = pd.concat([df, new_cols], axis=1)
                        st.session_state.data_store[active_k] = df
                        st.success(f"Kolom {split_col} berhasil dipecah.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal split: {e}")

            # B. MERGE COLUMN
            with st.expander("B. Merge Column (Gabung Kolom)"):
                merge_cols = st.multiselect("Pilih Beberapa Kolom:", df.columns)
                separator = st.text_input("Pemisah Gabungan", " ", key="merge_sep")
                new_col_name = st.text_input("Nama Kolom Baru", "Gabungan_Baru")
                
                if st.button("Gabung Kolom"):
                    if not merge_cols:
                        st.error("Pilih minimal 2 kolom.")
                    else:
                        # Gabung dengan convert ke string dulu
                        df[new_col_name] = df[merge_cols].astype(str).agg(separator.join, axis=1)
                        st.session_state.data_store[active_k] = df
                        st.success(f"Kolom baru '{new_col_name}' berhasil dibuat.")
                        st.rerun()

            # C. DATA TYPE FORMATTING
            with st.expander("C. Change Data Type (Ubah Tipe Data)"):
                c_type1, c_type2 = st.columns(2)
                type_col = c_type1.selectbox("Pilih Kolom:", df.columns, key="type_col")
                target_type = c_type2.selectbox("Ubah ke Tipe:", ["String (Teks)", "Integer (Angka Bulat)", "Float (Desimal)", "DateTime (Tanggal)"])
                
                if st.button("Ubah Tipe Data"):
                    try:
                        if "String" in target_type:
                            df[type_col] = df[type_col].astype(str)
                        elif "Integer" in target_type:
                            df[type_col] = pd.to_numeric(df[type_col], errors='coerce').fillna(0).astype(int)
                        elif "Float" in target_type:
                            df[type_col] = pd.to_numeric(df[type_col], errors='coerce')
                        elif "DateTime" in target_type:
                            df[type_col] = pd.to_datetime(df[type_col], errors='coerce')
                        
                        st.session_state.data_store[active_k] = df
                        st.success(f"Kolom {type_col} berhasil diubah ke {target_type}.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal mengubah tipe: {e}")

        # --- TAB 4: RELATIONAL (JOIN) ---
        with t4:
            st.subheader("Relational Operations (Join Tables)")
            
            # Cek apakah ada data lain untuk diajak join
            other_keys = [k for k in st.session_state.data_store.keys() if k != active_k]
            
            if not other_keys:
                st.warning("⚠️ Anda butuh minimal 2 data yang sudah di-load untuk melakukan Join. Silakan Extract data lain dulu.")
            else:
                st.info(f"Menggabungkan Tabel Aktif (**{active_k}**) dengan Tabel Lain.")
                
                c_j1, c_j2 = st.columns(2)
                right_table_name = c_j1.selectbox("Pilih Tabel Pasangan (Right):", other_keys)
                join_type = c_j2.selectbox("Jenis Join:", ["left", "inner", "right", "outer"])
                
                right_df = st.session_state.data_store[right_table_name]
                
                c_j3, c_j4 = st.columns(2)
                left_on = c_j3.selectbox(f"Kunci di {active_k} (Left):", df.columns)
                right_on = c_j4.selectbox(f"Kunci di {right_table_name} (Right):", right_df.columns)
                
                if st.button("Lakukan Join"):
                    try:
                        merged_df = pd.merge(df, right_df, left_on=left_on, right_on=right_on, how=join_type)
                        
                        # Simpan sebagai data baru agar data asli tidak rusak
                        new_join_name = f"Join_{active_k}_{right_table_name}"
                        st.session_state.data_store[new_join_name] = merged_df
                        
                        st.success(f"Join Berhasil! Data baru tersimpan dengan nama: {new_join_name}")
                        st.write("Silakan pilih data baru tersebut di Sidebar untuk melihat hasilnya.")
                        
                        # Opsional: Langsung pindah ke data baru
                        st.session_state.active_key = new_join_name
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal melakukan Join: {e}")

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