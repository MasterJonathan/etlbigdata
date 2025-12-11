import streamlit as st
import pandas as pd
import io
from sqlalchemy import create_engine, inspect
import findspark
findspark.init()
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, concat_ws, lit, regexp_replace
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

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
        st.header(f"2. Transform: Mengedit '{active_k}' (Spark Engine)")
        
        # Initialize Spark
        spark = SparkSession.builder.appName("StreamlitETL").getOrCreate()
        
        # Get Pandas DF
        pdf = st.session_state.data_store[active_k]
        
        # Convert to Spark DF
        try:
            df = spark.createDataFrame(pdf)
        except Exception as e:
            st.warning(f"Gagal convert ke Spark secara langsung, mencoba convert semua ke String dulu: {e}")
            df = spark.createDataFrame(pdf.astype(str))

        # --- PREVIEW DATA ---
        with st.expander("🔍 Lihat Data Saat Ini", expanded=True):
            st.dataframe(pdf.head(5))
            st.caption(f"Total Baris: {pdf.shape[0]} | Total Kolom: {pdf.shape[1]}")
        
        # --- MENU TRANSFORMASI ---
        t1, t2, t3, t4 = st.tabs([
            "🧹 Cleaning", 
            "🔧 Manipulation", 
            "📝 Column Ops", 
            "🔗 Relational (Join)"
        ])
        
        # --- TAB 1: DATA CLEANING ---
        with t1:
            st.subheader("Data Cleaning (Spark)")
            col_c1, col_c2 = st.columns(2)
            
            with col_c1:
                st.markdown("**1. Fill The Blank**")
                fill_val = st.text_input("Isi data teks kosong dengan:", "Unknown")
                if st.button("Isi Data Kosong"):
                    # Spark fillna
                    df = df.na.fill(fill_val) # Fills strings
                    df = df.na.fill(0)        # Fills numbers
                    
                    st.session_state.data_store[active_k] = df.toPandas()
                    st.success("Data kosong berhasil diisi.")
                    st.rerun()
            
            with col_c2:
                st.markdown("**2. Remove Duplicates**")
                if st.button("Hapus Duplikat"):
                    before = df.count()
                    df = df.dropDuplicates()
                    after = df.count()
                    
                    st.session_state.data_store[active_k] = df.toPandas()
                    st.success(f"Berhasil menghapus {before - after} baris duplikat.")
                    st.rerun()

        # --- TAB 2: DATA MANIPULATION ---
        with t2:
            st.subheader("Data Manipulation (Spark)")
            
            # A. REPLACE
            with st.expander("A. Replace Value (Ganti Nilai)"):
                c_rep1, c_rep2, c_rep3 = st.columns(3)
                rep_col = c_rep1.selectbox("Pilih Kolom:", df.columns, key="rep_col")
                old_val = c_rep2.text_input("Nilai Lama")
                new_val = c_rep3.text_input("Nilai Baru")
                
                if st.button("Ganti Nilai"):
                    # Spark replace
                    df = df.withColumn(rep_col, when(col(rep_col) == old_val, new_val).otherwise(col(rep_col)))
                    st.session_state.data_store[active_k] = df.toPandas()
                    st.success(f"Mengganti '{old_val}' menjadi '{new_val}'")
                    st.rerun()

            # B. FILTER
            with st.expander("B. Filter Data (Saring)"):
                c_fil1, c_fil2 = st.columns(2)
                fil_col = c_fil1.selectbox("Filter Berdasarkan Kolom:", df.columns, key="fil_col")
                fil_val = c_fil2.text_input("Nilai yang dicari (Contains):")
                
                if st.button("Terapkan Filter"):
                    # Spark filter contains
                    df = df.filter(col(fil_col).contains(fil_val))
                    st.session_state.data_store[active_k] = df.toPandas()
                    st.success(f"Filter diterapkan.")
                    st.rerun()

            # C. TRANSPOSE
            with st.expander("C. Transpose (Putar Baris <> Kolom)"):
                st.warning("⚠️ Transpose tidak didukung secara native di Spark untuk UI ini (kembali ke Pandas).")
                if st.button("Lakukan Transpose"):
                    pdf = df.toPandas()
                    pdf = pdf.T.reset_index()
                    st.session_state.data_store[active_k] = pdf
                    st.success("Transpose berhasil (via Pandas).")
                    st.rerun()

        # --- TAB 3: COLUMN OPERATIONS ---
        with t3:
            st.subheader("Column Operations (Spark)")
            
            # A. SPLIT COLUMN
            with st.expander("A. Split Column (Pecah Kolom)"):
                split_col = st.selectbox("Pilih Kolom untuk Dipecah:", df.columns, key="split_col")
                delimiter = st.text_input("Pemisah (Delimiter)", " ", help="Contoh: koma (,), spasi ( ), strip (-)")
                
                if st.button("Pecah Kolom"):
                    try:
                        # Simple split implementation: take first 2 parts
                        split_col_expr = split(col(split_col), delimiter)
                        df = df.withColumn(f"{split_col}_1", split_col_expr.getItem(0)) \
                               .withColumn(f"{split_col}_2", split_col_expr.getItem(1))
                        
                        st.session_state.data_store[active_k] = df.toPandas()
                        st.success(f"Kolom {split_col} berhasil dipecah (Max 2 bagian).")
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
                        df = df.withColumn(new_col_name, concat_ws(separator, *[col(c) for c in merge_cols]))
                        st.session_state.data_store[active_k] = df.toPandas()
                        st.success(f"Kolom baru '{new_col_name}' berhasil dibuat.")
                        st.rerun()

            # C. DATA TYPE FORMATTING
            with st.expander("C. Change Data Type (Ubah Tipe Data)"):
                c_type1, c_type2 = st.columns(2)
                type_col = c_type1.selectbox("Pilih Kolom:", df.columns, key="type_col")
                target_type = c_type2.selectbox("Ubah ke Tipe:", ["String", "Integer", "Float", "Date"])
                
                if st.button("Ubah Tipe Data"):
                    try:
                        if target_type == "String":
                            df = df.withColumn(type_col, col(type_col).cast(StringType()))
                        elif target_type == "Integer":
                            df = df.withColumn(type_col, col(type_col).cast(IntegerType()))
                        elif target_type == "Float":
                            df = df.withColumn(type_col, col(type_col).cast(FloatType()))
                        elif target_type == "Date":
                            df = df.withColumn(type_col, col(type_col).cast(DateType()))
                        
                        st.session_state.data_store[active_k] = df.toPandas()
                        st.success(f"Kolom {type_col} berhasil diubah ke {target_type}.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal mengubah tipe: {e}")

        # --- TAB 4: RELATIONAL (JOIN) ---
        with t4:
            st.subheader("Relational Operations (Join Tables - Spark)")
            
            other_keys = [k for k in st.session_state.data_store.keys() if k != active_k]
            
            if not other_keys:
                st.warning("⚠️ Anda butuh minimal 2 data.")
            else:
                c_j1, c_j2 = st.columns(2)
                right_table_name = c_j1.selectbox("Pilih Tabel Pasangan (Right):", other_keys)
                join_type = c_j2.selectbox("Jenis Join:", ["left", "inner", "right", "outer"]) # Spark supports these
                
                right_pdf = st.session_state.data_store[right_table_name]
                try:
                    right_df = spark.createDataFrame(right_pdf)
                except:
                    right_df = spark.createDataFrame(right_pdf.astype(str))
                
                c_j3, c_j4 = st.columns(2)
                left_on = c_j3.selectbox(f"Kunci di {active_k} (Left):", df.columns)
                right_on = c_j4.selectbox(f"Kunci di {right_table_name} (Right):", right_df.columns)
                
                if st.button("Lakukan Join"):
                    try:
                        merged_df = df.join(right_df, df[left_on] == right_df[right_on], how=join_type)
                        
                        # Drop duplicate key column if names are same to avoid confusion in Pandas
                        # Spark keeps both keys. Pandas merge usually merges them.
                        # For simplicity, we just convert back.
                        
                        new_join_name = f"Join_{active_k}_{right_table_name}"
                        st.session_state.data_store[new_join_name] = merged_df.toPandas()
                        
                        st.success(f"Join Berhasil! Data baru: {new_join_name}")
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
        
        target = st.selectbox("Target Simpan:", ["Hadoop (Parquet)", "MySQL Database", "HDFS (Spark)"])
        
        if target == "Hadoop (Parquet)":
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            st.download_button("Download Parquet", buffer.getvalue(), "hasil.parquet")
            
        elif target == "HDFS (Spark)":
            hdfs_url = st.text_input("HDFS URL", "hdfs://localhost:9000/user/royevan/uas")
            if st.button("Save to HDFS"):
                try:
                    spark = SparkSession.builder.appName("StreamlitETL").getOrCreate()
                    # Convert Pandas DF to Spark DF
                    df_spark = df.astype(str)
                    spark_df = spark.createDataFrame(df_spark)
                    
                    # Save as Text File
                    spark_df.rdd.map(lambda row: ",".join([str(x) for x in row])).saveAsTextFile(hdfs_url)
                    
                    st.success(f"Berhasil simpan ke HDFS: {hdfs_url}")
                except Exception as e:
                    st.error(f"Gagal simpan ke HDFS: {e}")

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