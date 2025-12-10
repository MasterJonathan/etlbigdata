import streamlit as st
import pandas as pd
import io

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="Simple ETL Tool", layout="wide")
st.title("🛠️ Simple ETL Tool (Pandas Version)")

# Inisialisasi Session State agar data tidak hilang saat klik tombol
if 'df' not in st.session_state:
    st.session_state.df = None

# ==========================================
# 1. EXTRACT (AMBIL DATA)
# ==========================================
st.header("1. Extract (Sumber Data)")
uploaded_file = st.file_uploader("Upload File (CSV atau Excel)", type=['csv', 'xlsx'])

if uploaded_file is not None:
    try:
        if uploaded_file.name.endswith('.csv'):
            df_temp = pd.read_csv(uploaded_file)
        else:
            df_temp = pd.read_excel(uploaded_file)
        
        # Simpan ke session state hanya jika belum ada data atau user upload baru
        if st.session_state.df is None: 
            st.session_state.df = df_temp
            st.success(f"File {uploaded_file.name} berhasil dimuat!")
    except Exception as e:
        st.error(f"Gagal membaca file: {e}")

# Tampilkan Data Frame jika sudah ada
if st.session_state.df is not None:
    df = st.session_state.df # Alias biar ngetiknya pendek
    
    st.markdown("### Preview Data:")
    st.dataframe(df.head(10))
    st.info(f"Total Baris: {df.shape[0]}, Total Kolom: {df.shape[1]}")

    # ==========================================
    # 2. TRANSFORM (OLAH DATA)
    # ==========================================
    st.header("2. Transform (Olah Data)")
    
    # --- Tab Menu agar rapi ---
    tab1, tab2, tab3 = st.tabs(["Cleaning", "Manipulasi Kolom", "Filter"])

    with tab1:
        st.subheader("Data Cleaning")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Isi Data Kosong (Fill NA)"):
                # Isi angka dengan 0, teks dengan "Unknown"
                num_cols = df.select_dtypes(include=['number']).columns
                obj_cols = df.select_dtypes(include=['object']).columns
                
                df[num_cols] = df[num_cols].fillna(0)
                df[obj_cols] = df[obj_cols].fillna("Unknown")
                
                st.session_state.df = df
                st.success("Data kosong berhasil diisi!")
                st.rerun()

        with col2:
            if st.button("Hapus Duplikat"):
                before = len(df)
                df = df.drop_duplicates()
                st.session_state.df = df
                st.success(f"Berhasil menghapus {before - len(df)} baris duplikat.")
                st.rerun()

    with tab2:
        st.subheader("Operasi Kolom")
        target_col = st.selectbox("Pilih Kolom:", df.columns)
        
        # Opsi Ganti Tipe Data
        new_type = st.selectbox("Ubah Tipe Data ke:", ["String", "Angka (Int)", "Angka (Float)"])
        if st.button("Ubah Tipe Data"):
            try:
                if new_type == "String":
                    df[target_col] = df[target_col].astype(str)
                elif new_type == "Angka (Int)":
                    df[target_col] = pd.to_numeric(df[target_col], errors='coerce').fillna(0).astype(int)
                else:
                    df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
                
                st.session_state.df = df
                st.success(f"Kolom {target_col} diubah ke {new_type}")
                st.rerun()
            except Exception as e:
                st.error(f"Gagal ubah tipe: {e}")

    with tab3:
        st.subheader("Filter Data")
        # Filter sederhana berdasarkan nilai kolom
        filter_col = st.selectbox("Filter Berdasarkan Kolom:", df.columns, key='filter_col')
        unique_vals = df[filter_col].unique()
        selected_val = st.selectbox("Pilih Nilai:", unique_vals)
        
        if st.button("Terapkan Filter"):
            df = df[df[filter_col] == selected_val]
            st.session_state.df = df
            st.success(f"Data difilter berdasarkan {filter_col} = {selected_val}")
            st.rerun()
            
        if st.button("Reset Data (Undo All)"):
            st.session_state.df = None
            st.rerun()

    # ==========================================
    # 3. LOAD (SIMPAN DATA)
    # ==========================================
    st.header("3. Load (Download Hasil)")
    
    st.write("Simpan hasil olahan ke file lokal (CSV/Excel).")
    
    col_dl1, col_dl2 = st.columns(2)
    
    with col_dl1:
        # Konversi ke CSV untuk download
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download sebagai CSV",
            data=csv,
            file_name='hasil_etl.csv',
            mime='text/csv',
        )
        
    with col_dl2:
        # Konversi ke Excel untuk download (butuh io buffer)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            
        st.download_button(
            label="Download sebagai Excel",
            data=buffer,
            file_name='hasil_etl.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

else:
    st.info("👋 Silakan upload file di bagian Extract untuk memulai.")
