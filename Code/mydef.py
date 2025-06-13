import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.patches as mpatches
import os


# Fungsi untuk mengkonversi file antara format CSV dan Parquet
def convert_file(path_input, path_output, format_target, save_index=False):
    
    # Mengkonversi file antara format CSV dan Parquet.

    # Args:
    #     path_input (str): Path lengkap ke file input.
    #     path_output (str): Path lengkap untuk menyimpan file output.
    #     format_target (str): Format output yang diinginkan. Ketik 'parquet' untuk 
    #                          konversi CSV ke Parquet, atau 'csv' untuk konversi 
    #                          Parquet ke CSV.
    #     simpan_index (bool): Jika True, indeks DataFrame akan ditulis ke file output.
    #                          Jika False (default), indeks tidak ditulis.
    #                          Untuk CSV ke Parquet, ini mengontrol apakah indeks dari CSV
    #                          (jika ada dan dibaca) akan disimpan. Seringkali DataFrame
    #                          dari CSV akan memiliki RangeIndex default yang tidak perlu disimpan.
    #                          Untuk Parquet ke CSV, ini mengontrol apakah indeks dari Parquet
    #                          (jika ada) akan ditulis ke CSV.

    # Returns:
    #     bool: True jika konversi berhasil, False jika gagal.
    
    try:
        if format_target.lower() == 'parquet':
            # Mode: Konversi dari CSV ke Parquet
            print(f"Membaca file CSV dari: {path_input}...")
            df = pd.read_csv(path_input)
            print("File CSV berhasil dibaca.")
            
            print(f"Menyimpan ke file Parquet di: {path_output}...")
            df.to_parquet(path_output, index=save_index)
            print("File berhasil disimpan sebagai Parquet.\n")
            return True

        elif format_target.lower() == 'csv':
            # Mode: Konversi dari Parquet ke CSV
            print(f"Membaca file Parquet dari: {path_input}...")
            df = pd.read_parquet(path_input)
            print("File Parquet berhasil dibaca.")
            
            print(f"Menyimpan ke file CSV di: {path_output}...")
            df.to_csv(path_output, index=save_index)
            print("File berhasil disimpan sebagai CSV.\n")
            return True
            
        else:
            print(f"ERROR: Pilihan format_target '{format_target}' tidak valid. Harap pilih 'csv' atau 'parquet'.")
            return False

    except FileNotFoundError:
        print(f"ERROR: File input tidak ditemukan di path: {path_input}")
        return False
    except Exception as e:
        print(f"Terjadi error saat konversi: {e}")
        return False

### Def Menghapus kolom-kolom tertentu dari DataFrame (data_1) ###
def remove_column(df_input, daftar_kolom_untuk_dihapus, nama_df="DataFrame"):
    
    # Menghapus kolom-kolom tertentu dari sebuah DataFrame, dengan output pemisah otomatis.

    # Args:
    #     df_input (pd.DataFrame): DataFrame input yang akan dimodifikasi.
    #     daftar_kolom_untuk_dihapus (list): List berisi nama-nama kolom (string) 
    #                                        yang ingin dihapus dari DataFrame.
    #     nama_df (str, opsional): Nama DataFrame yang sedang diproses, untuk ditampilkan
    #                              di pesan pemisah. Defaultnya "DataFrame".

    # Returns:
    #     pd.DataFrame: DataFrame baru dengan kolom-kolom yang sudah dihapus.
    #                   Mengembalikan salinan DataFrame asli jika terjadi error 
    #                   atau tidak ada kolom yang valid untuk dihapus.
    
    print(f"\n--- Memulai Proses Penghapusan Kolom untuk: {nama_df} ---") # Pemisah Awal
    try:
        shape_asli = df_input.shape
        print(f"Bentuk DataFrame SEBELUM penghapusan kolom: {shape_asli}")
        
        kolom_valid_untuk_dihapus = [
            kolom for kolom in daftar_kolom_untuk_dihapus if kolom in df_input.columns
        ]
        
        kolom_tidak_ditemukan = [
            kolom for kolom in daftar_kolom_untuk_dihapus if kolom not in df_input.columns
        ]

        if kolom_tidak_ditemukan:
            print(f"PERINGATAN: Kolom berikut dari daftar yang ingin dihapus tidak ditemukan di DataFrame: {kolom_tidak_ditemukan}")

        if not kolom_valid_untuk_dihapus:
            print("Tidak ada kolom dari daftar yang valid untuk dihapus. DataFrame tidak berubah.")
            print(f"Kolom yang ada saat ini: {df_input.columns.tolist()}")
            print(f"--- Proses Penghapusan Kolom untuk {nama_df} Selesai (Tidak Ada Perubahan) ---\n") # Pemisah Akhir
            return df_input.copy() 

        print(f"Kolom yang akan dihapus: {kolom_valid_untuk_dihapus}")
        df_hasil = df_input.drop(columns=kolom_valid_untuk_dihapus)
        print("Kolom berhasil dihapus.")
        print(f"Bentuk DataFrame SETELAH penghapusan kolom: {df_hasil.shape}")
        
        kolom_tersisa = df_hasil.columns.tolist()
        print(f"Kolom yang tersisa ({len(kolom_tersisa)} kolom): {kolom_tersisa}")
        
        print(f"--- Proses Penghapusan Kolom untuk {nama_df} Selesai dengan Sukses ---\n") # Pemisah Akhir
        return df_hasil

    except Exception as e:
        print(f"Terjadi error saat menghapus kolom: {e}")
        print(f"--- Proses Penghapusan Kolom untuk {nama_df} Gagal dengan Error ---\n") # Pemisah Akhir jika error
        return df_input.copy()

# Fungsi untuk menganalisis dan memvisualisasikan missing data dari file pivot
def visualisasi_missing_data_pivot_table(
    path_file_pivot, 
    path_file_info_meter=None, 
    figsize_heatmap=(30, 15), # Sesuai contoh Bapak
    ytick_interval=50,       # Untuk yticklabels, sesuai contoh Bapak
    grid_horizontal_interval=30, # Interval grid horizontal, sesuai contoh Bapak
    tampilkan_plot=True, 
    path_output_tabel_csv=None, 
    path_output_heatmap_png=None
):
    """
    Menganalisis dan memvisualisasikan missing data dari file data pivot,
    dengan tampilan heatmap disesuaikan agar mirip dengan contoh kode pengguna.

    Args:
        path_file_pivot (str): Path ke file Parquet/CSV data pivot.
        path_file_info_meter (str, opsional): Path ke file CSV info meter_id.
        figsize_heatmap (tuple, opsional): Ukuran gambar heatmap.
        ytick_interval (int, opsional): Interval untuk label sumbu Y pada heatmap.
        grid_horizontal_interval (int, opsional): Interval untuk garis grid horizontal.
        tampilkan_plot (bool, opsional): True untuk tampilkan plot.
        path_output_tabel_csv (str, opsional): Path simpan tabel missing data CSV.
        path_output_heatmap_png (str, opsional): Path simpan gambar heatmap PNG.

    Returns:
        pandas.DataFrame: DataFrame analisis missing data, atau None jika error.
    """
    print(f"\n--- Memulai Analisis Missing Data untuk File: {path_file_pivot} (Versi Disesuaikan) ---")
    df_info_meter_clean = None 
    try:
        # 1. Memuat data pivot (sama seperti fungsi sebelumnya)
        try:
            df_pivot = pd.read_parquet(path_file_pivot)
            print("File pivot berhasil dimuat sebagai Parquet.")
        except Exception:
            try:
                df_pivot = pd.read_csv(path_file_pivot, index_col=0)
                if not isinstance(df_pivot.index, pd.DatetimeIndex):
                    df_pivot.index = pd.to_datetime(df_pivot.index)
                print("File pivot berhasil dimuat sebagai CSV.")
            except Exception as e_csv:
                print(f"ERROR: Gagal memuat file pivot. Error: {e_csv}")
                print("--- Analisis Missing Data Gagal ---")
                return None
        
        if df_pivot.empty:
            print("PERINGATAN: File pivot kosong.")
            print("--- Analisis Missing Data Selesai (File Kosong) ---")
            return pd.DataFrame()

        # Pastikan kolom meter_id di df_pivot adalah string untuk konsistensi lookup
        df_pivot.columns = df_pivot.columns.astype(str)

        # 2. Menghitung data kosong (sama seperti sebelumnya)
        missing_counts = df_pivot.isna().sum().reset_index()
        missing_counts.columns = ['meter_id', 'jumlah_missing']
        missing_counts['meter_id'] = missing_counts['meter_id'].astype(str)

        # 3. Memuat & memproses file informasi meter_id (sama seperti sebelumnya)
        tabel_analisis_missing = missing_counts.copy()
        if path_file_info_meter:
            try:
                # Jika file info meter adalah Parquet seperti di contoh kode Bapak:
                if path_file_info_meter.endswith('.parquet'):
                    df_info_meter_raw = pd.read_parquet(path_file_info_meter)
                else: # Asumsi CSV jika bukan Parquet
                    df_info_meter_raw = pd.read_csv(path_file_info_meter, delimiter=';')
                
                print(f"File informasi meter berhasil dimuat dari: {path_file_info_meter}")
                df_info_meter_clean = df_info_meter_raw.copy()
                df_info_meter_clean.columns = df_info_meter_clean.columns.str.strip().str.lower().str.replace(' ', '_')
                
                if 'meter_id' not in df_info_meter_clean.columns:
                    print(f"PERINGATAN: Kolom 'meter_id' tidak ditemukan di {path_file_info_meter}.")
                    df_info_meter_clean = None 
                else:
                    df_info_meter_clean['meter_id'] = df_info_meter_clean['meter_id'].astype(str)
                    tabel_analisis_missing = pd.merge(missing_counts, df_info_meter_clean, on='meter_id', how='left')
            except FileNotFoundError:
                print(f"PERINGATAN: File informasi meter tidak ditemukan di: {path_file_info_meter}.")
            except Exception as e_info:
                print(f"Terjadi error saat memproses file info meter: {e_info}.")
        
        # 4. Menghitung persentase missing (sama seperti sebelumnya)
        if not df_pivot.empty:
            tabel_analisis_missing['persentase_missing'] = (tabel_analisis_missing['jumlah_missing'] / len(df_pivot)) * 100.0
        else:
            tabel_analisis_missing['persentase_missing'] = 0.0
        tabel_analisis_missing = tabel_analisis_missing.sort_values(by='persentase_missing', ascending=False)

        print("\nRingkasan Analisis Missing Data (beberapa baris teratas):")
        print(tabel_analisis_missing.head().to_string())

        if path_output_tabel_csv:
            # ... (kode penyimpanan tabel CSV, sama seperti sebelumnya, termasuk os.makedirs) ...
            try:
                output_dir_csv = os.path.dirname(path_output_tabel_csv)
                if output_dir_csv and not os.path.exists(output_dir_csv):
                    os.makedirs(output_dir_csv); print(f"Direktori dibuat: {output_dir_csv}")
                tabel_analisis_missing.to_csv(path_output_tabel_csv, index=False)
                print(f"Tabel analisis missing data disimpan ke: {path_output_tabel_csv}")
            except Exception as e_save_csv:
                print(f"ERROR: Gagal menyimpan tabel analisis: {e_save_csv}")

        # --- PEMBUATAN HEATMAP DISESUAIKAN DENGAN KODE BAPAK ---
        print("\nMembuat heatmap missing data (gaya disesuaikan)...")
        
        # Membuat DataFrame baru dengan nama kolom yang sudah diubah untuk plotting
        # Ini meniru langkah rename columns di kode Bapak
        df_pivot_renamed = df_pivot.copy()
        if df_info_meter_clean is not None and 'meter_id' in df_info_meter_clean.columns and \
           ('gedung' in df_info_meter_clean.columns or 'lokasi' in df_info_meter_clean.columns):
            
            meter_id_mapping = {}
            for _, row in df_info_meter_clean.iterrows():
                # Pastikan 'gedung' dan 'lokasi' ada, jika tidak, beri string kosong
                gedung = str(row.get('gedung', ''))
                lokasi = str(row.get('lokasi', ''))
                # Format seperti di kode Bapak: "gedung lokasi" (spasi tunggal)
                # Jika salah satu kosong, misal "gedung ", maka .strip() akan membantu.
                # Jika keduanya kosong, hasilnya string kosong, perlu fallback.
                label_parts = [part for part in [gedung, lokasi] if part] # Ambil yg tidak kosong
                label = " ".join(label_parts) if label_parts else str(row['meter_id'])
                meter_id_mapping[str(row['meter_id'])] = label
            
            df_pivot_renamed.rename(columns=meter_id_mapping, inplace=True)
            print("Nama kolom diubah menjadi 'Gedung Lokasi' untuk heatmap.")
        else:
            print("Info meter tidak tersedia atau tidak lengkap, menggunakan meter_id asli untuk sumbu X heatmap.")

        data_untuk_heatmap = df_pivot_renamed.isna() # Boolean, True untuk NaN

        # Bapak bisa mengatur font global di luar fungsi jika diperlukan
        # plt.rcParams['font.family'] = 'Arial' 
        
        plt.figure(figsize=figsize_heatmap)
        ax = sns.heatmap(data_untuk_heatmap, cbar=False, cmap='viridis', yticklabels=ytick_interval)

        # Tambahkan garis vertikal dan horizontal (sesuai kode Bapak)
        for i in range(1, data_untuk_heatmap.shape[1]):
            ax.axvline(x=i, color='black', linewidth=0.5)
        for j in range(1, data_untuk_heatmap.shape[0], grid_horizontal_interval):
            ax.axhline(y=j, color='black', linewidth=0.3)

        # Tambahkan legend custom (sesuai kode Bapak)
        # Warna 'yellow' dan 'purple' ini adalah interpretasi dari cmap 'viridis' untuk boolean
        # dimana True (NaN/kosong) biasanya kuning, False (ada data) biasanya ungu.
        yellow_patch = mpatches.Patch(color='yellow', label='Data Kosong (NaN)')
        purple_patch = mpatches.Patch(color='purple', label='Ada Data (Tidak Kosong)')
        ax.legend(
            handles=[yellow_patch, purple_patch],
            bbox_to_anchor=(1, 1.05), # Sesuai kode Bapak
            ncol=2,
            fontsize=16,
            frameon=False
        )

        ax.set_title('Heatmap of Missing Data per Gedung + Lokasi and Timestamp', fontsize=20)
        ax.set_xlabel('Gedung + Lokasi', fontsize=16)
        ax.set_ylabel('Timestamp', fontsize=16)
        plt.xticks(rotation=90, ha='right') # ha='right' agar align dengan tick
        
        # Menggunakan plt.tight_layout() seperti di kode Bapak
        # Mungkin perlu sedikit penyesuaian manual jika legenda masih terpotong
        try:
            plt.tight_layout() 
        except Exception as e_layout:
            print(f"Peringatan saat menjalankan tight_layout: {e_layout}")


        if path_output_heatmap_png:
            try:
                output_dir_png = os.path.dirname(path_output_heatmap_png)
                if output_dir_png and not os.path.exists(output_dir_png):
                    os.makedirs(output_dir_png); print(f"Direktori dibuat: {output_dir_png}")
                plt.savefig(path_output_heatmap_png, bbox_inches='tight') # bbox_inches='tight' direkomendasikan
                print(f"Heatmap missing data disimpan ke: {path_output_heatmap_png}")
            except Exception as e_save_png:
                print(f"ERROR: Gagal menyimpan heatmap: {e_save_png}")

        if tampilkan_plot:
            plt.show()
        else:
            plt.close() 

        print("--- Analisis Missing Data Selesai ---")
        return tabel_analisis_missing

    except Exception as e:
        print(f"Terjadi ERROR utama saat analisis missing data: {e}")
        import traceback 
        traceback.print_exc() 
        print("--- Analisis Missing Data Gagal ---")
        return None

# Fungsi mengisi data kosong pada DataFrame pivot dengan backward fill (bfill)
# dan melakukan filter kolom meter_id yang memiliki data kosong <= 10%
def process_data_with_bfill(file_path, info_meter_path=r'C:\MyFolder\Git\TA_SpatioTemporal\Data\parquet\meter_id.parquet'):
    """
    Memproses data konsumsi energi dari file CSV atau Parquet:
    1. Memuat data secara dinamis (CSV/Parquet), menggunakan kolom 'timestamp' sebagai index waktu.
    2. Kolom-kolom lain dianggap sebagai meter_id.
    3. Memfilter kolom meter_id dengan data kosong lebih dari 10%.
    4. Menerapkan backward fill (bfill) pada NaN yang tersisa secara kolom.
    5. Mencetak statistik ringkasan dan cuplikan DataFrame yang diproses.
    6. Menampilkan daftar meter_id (beserta lokasi & gedung jika info tersedia) yang dihapus.
    """
    try:
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.csv':
            df = pd.read_csv(file_path, index_col='timestamp')
        elif file_extension == '.parquet':
            df = pd.read_parquet(file_path)
            
            # --- PERBAIKAN LOGIKA DIMULAI DI SINI ---
            # Cek 1: Jika 'timestamp' sudah menjadi index, tidak perlu melakukan apa-apa.
            if df.index.name == 'timestamp':
                print(f"Info: File Parquet '{os.path.basename(file_path)}' sudah memiliki index 'timestamp'.")
            # Cek 2: Jika tidak, cek apakah ada kolom 'timestamp' untuk dijadikan index.
            elif 'timestamp' in df.columns:
                df = df.set_index('timestamp')
            # Error: Jika keduanya tidak terpenuhi.
            else:
                raise KeyError("Kolom atau index bernama 'timestamp' tidak ditemukan di file Parquet.")
            # --- PERBAIKAN LOGIKA SELESAI ---

        else:
            print(f"Error: Format file tidak didukung untuk '{file_path}'. Harap gunakan .csv atau .parquet.")
            return None
        
        df.index = pd.to_datetime(df.index)

    except FileNotFoundError:
        print(f"Error: File '{file_path}' tidak ditemukan.")
        return None
    except KeyError as e: # Tangkap error dari raise di atas
        print(f"Error pada file '{os.path.basename(file_path)}': {e}")
        print("Pastikan file Anda memiliki kolom atau index bernama 'timestamp' untuk data waktu.")
        return None
    except Exception as e:
        print(f"Error saat membaca atau memproses file: {e}")
        return None

    # (Sisa kode fungsi sama persis seperti sebelumnya, tidak perlu diubah)
    print(f"\nMemproses file data energi: {file_path}")
    print(f"Index (timestamp) terdeteksi. Jumlah data waktu: {len(df.index)}")

    if df.empty or len(df.columns) == 0:
        print("Data kosong atau tidak ada kolom meter_id setelah memuat. Proses dihentikan.")
        return None

    initial_meter_id_cols_count = len(df.columns)
    initial_total_nans = df.isnull().sum().sum()
    print(f"\nJumlah kolom meter_id awal: {initial_meter_id_cols_count}")
    print(f"Total data kosong awal di semua meter_id: {initial_total_nans}")

    num_time_steps = len(df.index)
    if num_time_steps == 0:
        print("Error: Tidak ada baris data (langkah waktu) dalam tabel.")
        return None

    missing_percentage_per_meter = df.isnull().sum(axis=0) / num_time_steps * 100

    meter_cols_to_keep = missing_percentage_per_meter[missing_percentage_per_meter <= 10].index
    meter_cols_removed = missing_percentage_per_meter[missing_percentage_per_meter > 10].index.tolist()
    df_filtered = df[meter_cols_to_keep].copy()

    removed_meter_cols_count = initial_meter_id_cols_count - len(df_filtered.columns)
    nans_in_filtered_before_fill = df_filtered.isnull().sum().sum()

    print(f"\nFilter kolom meter_id berdasarkan >10% data kosong:")
    print(f"Jumlah kolom meter_id yang dihapus: {removed_meter_cols_count}")
    print(f"Jumlah kolom meter_id yang tersisa: {len(df_filtered.columns)}")
    print(f"Total data kosong pada meter_id tersisa (sebelum bfill): {nans_in_filtered_before_fill}")
    
    if meter_cols_removed:
        print("\nDaftar meter_id yang dihapus karena missing > 10%:")
        try:
            if info_meter_path.endswith('.parquet'):
                info_meter = pd.read_parquet(info_meter_path)
            else:
                info_meter = pd.read_csv(info_meter_path, delimiter=';')
            info_meter.columns = info_meter.columns.str.strip().str.lower().str.replace(' ', '_')
            info_meter['meter_id'] = info_meter['meter_id'].astype(str)
            meter_cols_removed_str = [str(x) for x in meter_cols_removed]
            removed_info = info_meter[info_meter['meter_id'].isin(meter_cols_removed_str)]
            not_found = set(meter_cols_removed_str) - set(removed_info['meter_id'])
            if not removed_info.empty:
                print(removed_info[['meter_id', 'gedung', 'lokasi']].to_string(index=False))
            if not_found:
                print("\nMeter_id berikut tidak ditemukan di tabel info_meter:")
                print(", ".join(not_found))
        except Exception as e:
            print(f"Gagal membaca info meter: {e}")
            print("Meter_id yang dihapus:")
            print(meter_cols_removed)
    else:
        print("Tidak ada meter_id yang dihapus pada proses ini.")

    if len(df_filtered.columns) == 0:
        print("\nTidak ada kolom meter_id yang tersisa setelah filter. Proses dihentikan.")
        return pd.DataFrame(index=df.index)

    df_filled_bfill = df_filtered.bfill(axis=0)

    nans_after_bfill = df_filled_bfill.isnull().sum().sum()
    print(f"\nProses Backward Fill (bfill) diterapkan per kolom meter_id:")
    print(f"Total data kosong setelah bfill: {nans_after_bfill}")

    if nans_after_bfill > 0:
        print("\nPERHATIAN: Masih ada data kosong setelah proses bfill.")
        print("Ini bisa terjadi jika ada nilai kosong di awal kolom (langkah waktu paling awal).")
    else:
        print("Semua data kosong pada kolom meter_id yang tersisa berhasil diisi dengan bfill.")

    print("\nCuplikan 5 baris pertama dari data hasil (setelah filter dan bfill):")
    print(df_filled_bfill.head().to_string())

    return df_filled_bfill

# Fungsi untuk memproses data suhu menjadi data per jam (rata-rata jam sebelumnya)
def proses_data_suhu_ringkas(path_input, path_output=None): # Menambahkan argumen path_output
    """
    Memproses file Parquet data suhu menjadi data per jam (rata-rata jam sebelumnya)
    dengan cara yang lebih ringkas.

    Args:
        path_input (str): Path ke file Parquet input.
        path_output (str, optional): Path untuk menyimpan file Parquet hasil. 
                                     Jika None, hasil tidak disimpan. Defaults to None.

    Returns:
        pandas.DataFrame: DataFrame dengan suhu rata-rata jam sebelumnya, atau None jika error.
    """
    try:
        # 1. Baca Parquet menggunakan path_input
        df = pd.read_parquet(path_input)
        
        # Konversi tipe, dan set index
        df['stationDateTime'] = pd.to_datetime(df['stationDateTime'])
        df['outsideTemp'] = pd.to_numeric(df['outsideTemp'], errors='coerce')
        df.dropna(subset=['outsideTemp'], inplace=True) # Hapus NaN setelah konversi suhu
        df.set_index('stationDateTime', inplace=True)

        # 2. Atasi duplikasi timestamp dengan mengambil rata-rata
        if not df.index.is_unique:
            df = df.groupby(df.index).mean()
        
        # 3. Resample per jam (rata-rata suhu jam tersebut), geser untuk mendapatkan rata-rata jam sebelumnya
        #    dan ubah nama kolom.
        #    Mengganti 'H' dengan 'h' untuk mengatasi FutureWarning
        df_per_jam = (df['outsideTemp']
                      .resample('h') # Resample ke frekuensi per jam (menggunakan 'h')
                      .mean()        # Hitung rata-rata suhu untuk setiap jam
                      .dropna()      # Hapus jam yang mungkin tidak memiliki data setelah resample
                      .shift(1, freq='h') # Geser data 1 jam ke depan (nilai jam X adalah rata-rata jam X-1) (menggunakan 'h')
                      .rename('avg_temp_previous_hour') # Ubah nama Series/kolom
                      .to_frame())   # Konversi kembali ke DataFrame

        print("\nData suhu per jam (rata-rata jam sebelumnya):")
        print(df_per_jam.head(10))

        # 4. Menyimpan hasil jika path_output diberikan
        if path_output:
            try:
                df_per_jam.to_parquet(path_output)
                df_per_jam.to_csv(path_output.replace('.parquet', '.csv'), index=True) # Simpan juga sebagai CSV
                print(f"Hasil disimpan ke: {path_output} dan {path_output.replace('.parquet', '.csv')}")
                print(f"\nHasil berhasil disimpan ke: {path_output}")
            except Exception as e:
                print(f"\nError saat menyimpan hasil ke '{path_output}': {e}")
        
        return df_per_jam

    except FileNotFoundError:
        print(f"Error: File Parquet tidak ditemukan di '{path_input}'")
        return None
    except KeyError as e:
        print(f"Error: Kolom 'stationDateTime' atau 'outsideTemp' tidak ditemukan. Detail: {e}")
        return None
    except Exception as e:
        print(f"Terjadi error: {e}")
        return None

# Fungsi untuk membuat plot suhu per jam dari file CSV dalam bentuk garis (line plot)
def plot_suhu_perjam_garis(
    file_path, 
    kolom_waktu='stationDateTime', 
    kolom_suhu='avg_temp_previous_hour',
    judul_plot='Visualisasi Suhu per Jam',
    label_y='Suhu (Celsius)',
    label_x='Waktu',
    path_output_gambar=None
):
    """
    Membuat plot garis (line plot) dari data suhu per jam dari sebuah file CSV.

    Fungsi ini akan membaca file CSV, mengkonversi kolom waktu, dan memvisualisasikan
    data suhu terhadap waktu.

    Args:
        file_path (str): Path lengkap ke file CSV data suhu.
        kolom_waktu (str): Nama kolom yang berisi informasi timestamp.
        kolom_suhu (str): Nama kolom yang berisi data suhu.
        judul_plot (str): Judul yang akan ditampilkan di atas plot.
        label_y (str): Label untuk sumbu Y (sumbu vertikal).
        label_x (str): Label untuk sumbu X (sumbu horizontal).
        path_output_gambar (str, opsional): Path untuk menyimpan gambar plot. 
                                            Jika None, plot hanya akan ditampilkan.
    """
    print(f"\n--- Memulai Visualisasi Data dari: {file_path} ---")
    try:
        # 1. Membaca data dari file CSV
        df = pd.read_csv(file_path)
        print("File CSV berhasil dibaca.")

        # 2. Validasi dan Pra-pemrosesan Data
        # Memastikan kolom yang dibutuhkan ada di dalam DataFrame
        if kolom_waktu not in df.columns or kolom_suhu not in df.columns:
            print(f"ERROR: Kolom '{kolom_waktu}' atau '{kolom_suhu}' tidak ditemukan.")
            print(f"Kolom yang tersedia: {df.columns.tolist()}")
            return

        # Mengkonversi kolom waktu menjadi format datetime dan menjadikannya index
        # Ini adalah praktik terbaik untuk plotting data deret waktu
        df[kolom_waktu] = pd.to_datetime(df[kolom_waktu])
        df.set_index(kolom_waktu, inplace=True)
        print("Kolom waktu berhasil diproses dan dijadikan index.")

        # 3. Membuat Plot
        plt.style.use('seaborn-v0_8-whitegrid') # Menggunakan style agar plot terlihat bagus
        fig, ax = plt.subplots(figsize=(15, 7)) # Membuat figure dan axes untuk plot

        # Menggunakan fungsi .plot() dari pandas yang terintegrasi dengan matplotlib
        df[kolom_suhu].plot(ax=ax, color='blue', linewidth=1.5)

        # 4. Kustomisasi Plot (Judul, Label, Grid)
        ax.set_title(judul_plot, fontsize=16, fontweight='bold')
        ax.set_ylabel(label_y, fontsize=12)
        ax.set_xlabel(label_x, fontsize=12)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        plt.tight_layout() # Merapikan layout agar tidak ada label yang terpotong

        # 5. Menyimpan Plot (jika path output diberikan)
        if path_output_gambar:
            try:
                # Membuat direktori jika belum ada
                output_dir = os.path.dirname(path_output_gambar)
                if output_dir and not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                    print(f"Direktori dibuat: {output_dir}")
                
                # Menyimpan gambar
                plt.savefig(path_output_gambar, dpi=300, bbox_inches='tight')
                print(f"Plot berhasil disimpan di: {path_output_gambar}")
            except Exception as e:
                print(f"ERROR: Gagal menyimpan gambar. Detail: {e}")

        # 6. Menampilkan Plot
        print("Menampilkan plot...")
        plt.show()

    except FileNotFoundError:
        print(f"ERROR: File tidak ditemukan di path: {file_path}")
    except Exception as e:
        print(f"Terjadi error yang tidak terduga: {e}")
    finally:
        print("--- Proses Visualisasi Selesai ---")
        
# Fungsi untuk membuat plot suhu per jam dari file CSV dalam bentuk titik (scatter plot)
def plot_suhu_perjam_titik(
    file_path, 
    kolom_waktu='stationDateTime', 
    kolom_suhu='avg_temp_previous_hour',
    judul_plot='Visualisasi Suhu per Jam (Titik)',
    label_y='Suhu (Celsius)',
    label_x='Waktu',
    path_output_gambar=None,
    ukuran_titik=2, # Ukuran titik di plot
    warna_titik='#007ACC', # Biru cerah
    gaya_titik='o' # 'o' untuk lingkaran, bisa juga 'x', '+', 's' (kotak)
):
    """
    Membuat plot titik (scatter plot) dari data suhu per jam dari file CSV.

    Args:
        file_path (str): Path lengkap ke file CSV data suhu.
        kolom_waktu (str): Nama kolom yang berisi informasi timestamp.
        kolom_suhu (str): Nama kolom yang berisi data suhu.
        judul_plot (str): Judul yang akan ditampilkan di atas plot.
        label_y (str): Label untuk sumbu Y.
        label_x (str): Label untuk sumbu X.
        path_output_gambar (str, opsional): Path untuk menyimpan gambar plot.
        ukuran_titik (int): Ukuran dari setiap titik di plot.
        warna_titik (str): Warna dari titik (nama warna atau kode hex).
        gaya_titik (str): Gaya penanda/marker untuk titik.
    """
    print(f"\n--- Memulai Visualisasi Titik dari: {file_path} ---")
    try:
        # 1. Membaca dan memproses data (sama seperti sebelumnya)
        df = pd.read_csv(file_path)
        if kolom_waktu not in df.columns or kolom_suhu not in df.columns:
            print(f"ERROR: Kolom '{kolom_waktu}' atau '{kolom_suhu}' tidak ditemukan.")
            return
        df[kolom_waktu] = pd.to_datetime(df[kolom_waktu])
        df.set_index(kolom_waktu, inplace=True)
        print("Data berhasil dibaca dan diproses.")

        # 2. Membuat Plot
        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(15, 7))

        # --- INI BAGIAN YANG DIUBAH ---
        # Menggunakan ax.scatter() untuk membuat plot titik
        ax.scatter(
            df.index,                # Sumbu X menggunakan index waktu
            df[kolom_suhu],          # Sumbu Y menggunakan data suhu
            s=ukuran_titik,          # Mengatur ukuran titik
            color=warna_titik,       # Mengatur warna
            marker=gaya_titik,       # Mengatur bentuk penanda
            alpha=0.7                # Transparansi agar titik yang tumpang tindih terlihat
        )
        # -----------------------------

        # 3. Kustomisasi Plot (sama seperti sebelumnya)
        ax.set_title(judul_plot, fontsize=16, fontweight='bold')
        ax.set_ylabel(label_y, fontsize=12)
        ax.set_xlabel(label_x, fontsize=12)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        plt.tight_layout()

        # 4. Menyimpan dan menampilkan plot (sama seperti sebelumnya)
        if path_output_gambar:
            # ... (logika penyimpanan sama persis) ...
            plt.savefig(path_output_gambar, dpi=300, bbox_inches='tight')
            print(f"Plot berhasil disimpan di: {path_output_gambar}")
        
        print("Menampilkan plot...")
        plt.show()

    except FileNotFoundError:
        print(f"ERROR: File tidak ditemukan di path: {file_path}")
    except Exception as e:
        print(f"Terjadi error yang tidak terduga: {e}")
    finally:
        print("--- Proses Visualisasi Titik Selesai ---")
        
# Fungsi untuk merangkum data kosong per bulan (Versi Definitif)
def rangkum_missing_data_per_bulan(
    file_path, 
    kolom_waktu='stationDateTime', 
    kolom_target='avg_temp_previous_hour',
    frekuensi_harapan='h'
):
    """
    Menganalisis data kosong per bulan dari file Parquet atau CSV,
    bahkan jika kolom waktu sudah menjadi index di file Parquet.
    """
    print(f"\n--- Memulai Rangkuman Data Kosong per Bulan untuk: {file_path} ---")
    try:
        # 1. Membaca file berdasarkan ekstensinya
        if file_path.lower().endswith('.parquet'):
            df = pd.read_parquet(file_path)
        elif file_path.lower().endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            print(f"ERROR: Tipe file tidak didukung. Harap gunakan .csv atau .parquet.")
            return None
        print("Data berhasil dibaca.")

        # 2. Membersihkan nama kolom dari spasi
        df.columns = df.columns.str.strip()
        
        # --- PERBAIKAN DEFINITIF: Cek apakah kolom waktu ada di kolom atau sudah menjadi index ---
        if kolom_waktu not in df.columns:
            print(f"'{kolom_waktu}' tidak ditemukan di kolom. Mengecek apakah ia adalah index...")
            if df.index.name == kolom_waktu:
                print("Benar, kolom waktu sudah menjadi index. Melanjutkan proses.")
                if not pd.api.types.is_datetime64_any_dtype(df.index):
                    print("Mengonversi index menjadi tipe Datetime...")
                    df.index = pd.to_datetime(df.index)
            else:
                print(f"ERROR: Kolom '{kolom_waktu}' tidak ditemukan baik di kolom maupun di index.")
                return None
        else:
            print(f"Kolom waktu '{kolom_waktu}' ditemukan di kolom. Menjadikannya sebagai index...")
            df[kolom_waktu] = pd.to_datetime(df[kolom_waktu])
            df.set_index(kolom_waktu, inplace=True)
        # --- AKHIR BLOK PERBAIKAN ---

        # 3. Proses sisa data
        df[kolom_target] = pd.to_numeric(df[kolom_target], errors='coerce')
        print("Pra-pemrosesan data selesai.")
        
        # Resample dan kalkulasi (tidak ada perubahan di sini)
        print(f"Me-resample data ke frekuensi '{frekuensi_harapan}'...")
        df_resampled = df[kolom_target].resample(frekuensi_harapan).mean()
        
        print("Merangkum hasil per bulan...")
        expected_counts = df_resampled.resample('ME').size()
        missing_counts = df_resampled.resample('ME').apply(lambda x: x.isnull().sum())
        
        summary_df = pd.DataFrame({
            'Jumlah Titik Diharapkan': expected_counts,
            'Jumlah Titik Kosong': missing_counts
        })
        
        summary_df['Jumlah Titik Diharapkan'] = pd.to_numeric(summary_df['Jumlah Titik Diharapkan'])
        summary_df['Jumlah Titik Kosong'] = pd.to_numeric(summary_df['Jumlah Titik Kosong'])
        
        summary_df['Jumlah Titik Ada'] = summary_df['Jumlah Titik Diharapkan'] - summary_df['Jumlah Titik Kosong']
        summary_df['Persentase Kosong (%)'] = (summary_df['Jumlah Titik Kosong'] / summary_df['Jumlah Titik Diharapkan'] * 100).round(2)
        
        # Pastikan index adalah DatetimeIndex sebelum menggunakan strftime
        if not isinstance(summary_df.index, pd.DatetimeIndex):
            summary_df.index = pd.to_datetime(summary_df.index)
        summary_df.index = summary_df.index.strftime('%Y-%m')
                
        print("--- Rangkuman Selesai ---")
        return summary_df
    except FileNotFoundError:
        print(f"ERROR: File tidak ditemukan di path: {file_path}")
        return None
    except Exception as e:
        print(f"Terjadi error yang tidak terduga: {e}")
        import traceback
        traceback.print_exc()
        return None

# Fungsi untuk memproses data konsumsi listrik dengan menggabungkan data dari gabungan meter_id dalam satu gedung
def gabung_konsumsi_gedung_pivot_table(file_data_path, file_mapping_path):
    """
    Memproses file data pivot (wide format) dengan menggabungkan beberapa kolom (meter_id)
    berdasarkan file pemetaan. Kolom-kolom yang digabung akan dijumlahkan nilainya.

    Args:
        file_data_path (str): Path ke file data konsumsi (format .csv atau .parquet).
        file_mapping_path (str): Path ke file CSV pemetaan meter.

    Returns:
        pandas.DataFrame: Pivot table baru dengan kolom yang sudah digabungkan.
    """
    try:
        # --- LANGKAH 1: MEMBACA KEDUA FILE ---
        print(f"Membaca file data pivot dari: {file_data_path}")
        _, file_extension = os.path.splitext(file_data_path)

        if file_extension == '.csv':
            df_data = pd.read_csv(file_data_path, index_col=0) # Asumsi kolom pertama adalah timestamp
        elif file_extension == '.parquet':
            df_data = pd.read_parquet(file_data_path)
            # Jika timestamp bukan index, jadikan index
            if 'timestamp' in df_data.columns:
                df_data.set_index('timestamp', inplace=True)
        else:
            raise ValueError(f"Format file tidak didukung: '{file_extension}'. Gunakan .csv atau .parquet.")

        print("Membaca file pemetaan meter...")
        df_mapping = pd.read_csv(file_mapping_path, sep=';')

        # --- LANGKAH 2: STANDARISASI DAN PERSIAPAN ---
        # Mengubah nama kolom data menjadi numerik agar bisa dicocokkan
        df_data.columns = [pd.to_numeric(col, errors='coerce') for col in df_data.columns]
        # Menghapus kolom yang namanya bukan angka (jika ada)
        df_data = df_data.loc[:, df_data.columns.notna()]

        # Menstandarisasi nama kolom di file mapping
        try:
            mapping_cols = df_mapping.columns
            col_rename_dict = {mapping_cols[1]: 'meter_id_lama', mapping_cols[2]: 'meter_id_baru'}
            df_mapping.rename(columns=col_rename_dict, inplace=True)
            df_mapping['meter_id_lama'] = pd.to_numeric(df_mapping['meter_id_lama'], errors='coerce')
            df_mapping.dropna(subset=['meter_id_lama'], inplace=True)
            df_mapping['meter_id_lama'] = df_mapping['meter_id_lama'].astype(int)
        except IndexError:
            raise ValueError("File pemetaan harus memiliki setidaknya 3 kolom.")

        # Membuat dictionary untuk pemetaan: {'Gedung A': [101, 102], 'Gedung B': [201]}
        grup_pemetaan = df_mapping.groupby('meter_id_baru')['meter_id_lama'].apply(list).to_dict()
        print("\nBerhasil membuat grup pemetaan meter:")
        print(grup_pemetaan)

        # --- LANGKAH 3: MEMPROSES PENGGABUNGAN KOLOM ---
        print("\nMemulai proses penggabungan kolom...")
        df_hasil = pd.DataFrame(index=df_data.index)
        meter_sudah_diproses = set()

        # Iterasi setiap grup (misal: 'CADL', 'CAS')
        for nama_grup, daftar_meter in grup_pemetaan.items():
            # Cari meter dari grup ini yang benar-benar ada sebagai kolom di data
            kolom_untuk_dijumlah = [m for m in daftar_meter if m in df_data.columns]
            
            if kolom_untuk_dijumlah:
                print(f"  > Menjumlahkan {len(kolom_untuk_dijumlah)} kolom untuk grup '{nama_grup}'...")
                # Menjumlahkan nilai semua kolom ini per baris (axis=1)
                df_hasil[nama_grup] = df_data[kolom_untuk_dijumlah].sum(axis=1)
                # Tandai semua meter ini sudah diproses
                meter_sudah_diproses.update(kolom_untuk_dijumlah)

        # --- LANGKAH 4: MENYALIN KOLOM YANG TIDAK DIGABUNG ---
        meter_asli = set(df_data.columns)
        meter_tidak_diproses = meter_asli - meter_sudah_diproses
        print(f"\nMenyalin {len(meter_tidak_diproses)} kolom yang tidak digabungkan...")
        
        # Menggabungkan hasil penjumlahan dengan kolom-kolom sisa
        df_hasil = pd.concat([df_hasil, df_data[list(meter_tidak_diproses)]], axis=1)
        
        # Mengembalikan timestamp dari index menjadi kolom biasa
        df_hasil.reset_index(inplace=True)

        print("\nProses selesai. Pivot table baru berhasil dibuat.")
        print("Contoh hasil akhir:")
        print(df_hasil.head())

        return df_hasil

    except Exception as e:
        print(f"Terjadi error: {e}")
        return None

# Fungsi untuk mengganti ID meter dengan nama gedung berdasarkan file pemetaan
def ganti_id_ke_nama_gedung(data_path, mapping_path, id_column, name_column):
    """
    Mengganti nama kolom (meter ID) di file data dengan nama gedung dari file pemetaan.

    Args:
        data_path (str): Path ke file data konsumsi energi (.csv atau .parquet).
        mapping_path (str): Path ke file pemetaan ID ke nama gedung (.csv, .parquet, atau .xlsx).
        id_column (str): Nama kolom di file pemetaan yang berisi ID meter.
        name_column (str): Nama kolom di file pemetaan yang berisi nama gedung.

    Returns:
        pandas.DataFrame: DataFrame baru dengan nama kolom yang sudah diganti, 
                          atau None jika terjadi error.
    """
    # --- 1. Membaca File Data Utama ---
    try:
        print(f"Membaca file data: {data_path}")
        file_ext_data = os.path.splitext(data_path)[1].lower()
        if file_ext_data == '.csv':
            # Mengasumsikan kolom pertama adalah index timestamp
            df_data = pd.read_csv(data_path, index_col=0, parse_dates=True)
        elif file_ext_data == '.parquet':
            df_data = pd.read_parquet(data_path)
        else:
            print(f"Error: Format file data '{file_ext_data}' tidak didukung. Harap gunakan .csv atau .parquet.")
            return None
    except FileNotFoundError:
        print(f"Error: File data tidak ditemukan di '{data_path}'")
        return None

    # --- 2. Membaca File Pemetaan (Mapping) ---
    try:
        print(f"Membaca file pemetaan: {mapping_path}")
        file_ext_map = os.path.splitext(mapping_path)[1].lower()
        if file_ext_map == '.csv':
            df_mapping = pd.read_csv(mapping_path)
        elif file_ext_map == '.parquet':
            df_mapping = pd.read_parquet(mapping_path)
        elif file_ext_map in ['.xlsx', '.xls']:
            df_mapping = pd.read_excel(mapping_path)
        else:
            print(f"Error: Format file pemetaan '{file_ext_map}' tidak didukung. Harap gunakan .csv, .parquet, atau .xlsx.")
            return None
    except FileNotFoundError:
        print(f"Error: File pemetaan tidak ditemukan di '{mapping_path}'")
        return None

    # --- 3. Proses Pemetaan dan Penggantian Nama ---
    try:
        print("Membuat kamus pemetaan...")
        # Mengubah tipe data ID di file mapping menjadi string agar cocok dengan nama kolom
        df_mapping[id_column] = df_mapping[id_column].astype(str)
        
        # Membuat kamus dari {id: nama_gedung}
        mapping_dict = pd.Series(df_mapping[name_column].values, index=df_mapping[id_column]).to_dict()

        print("Mengganti nama kolom...")
        df_renamed = df_data.rename(columns=mapping_dict)
        
        print("\nProses penggantian nama kolom selesai dengan sukses!")
        return df_renamed

    except KeyError as e:
        print(f"Error: Kolom {e} tidak ditemukan di file pemetaan. Pastikan nama kolom sudah benar.")
        print(f"Kolom yang tersedia di file pemetaan: {df_mapping.columns.tolist()}")
        return None
    except Exception as e:
        print(f"Terjadi error tak terduga: {e}")
        return None

# Fungsi untuk mengolah data suhu dengan imputasi berdasarkan pola harian
def olah_data_suhu(
    file_input,
    file_output_csv,
    file_output_parquet,
    nama_kolom_timestamp='timestamp',
    nama_kolom_suhu='suhu'
):
    """
    Fungsi untuk membaca, mengolah, dan menyimpan data suhu.
    Mengembalikan DataFrame lengkap dengan kolom 'year' dan 'month' untuk visualisasi.
    """
    print(f"Mulai proses pengolahan data suhu dari file: '{file_input}'")

    try:
        # --- Baca data ---
        if file_input.endswith('.csv'):
            df = pd.read_csv(file_input)
        elif file_input.endswith('.parquet'):
            df = pd.read_parquet(file_input)
        else:
            raise ValueError("Format file input tidak didukung.")

        # Atur ulang indeks jika perlu dan ganti nama kolom
        if 'index' in df.columns and nama_kolom_timestamp not in df.columns:
            df.rename(columns={'index': nama_kolom_timestamp}, inplace=True)
        
        if nama_kolom_timestamp not in df.columns:
             # Jika kolom timestamp tidak ada setelah coba rename, coba reset index
            df.reset_index(inplace=True)
            if 'index' in df.columns and nama_kolom_timestamp not in df.columns:
                 df.rename(columns={'index': nama_kolom_timestamp}, inplace=True)

        if nama_kolom_timestamp not in df.columns or nama_kolom_suhu not in df.columns:
            raise KeyError(f"Kolom '{nama_kolom_timestamp}' atau '{nama_kolom_suhu}' tidak ditemukan.")

        df[nama_kolom_timestamp] = pd.to_datetime(df[nama_kolom_timestamp])

        # --- Filter outlier ---
        temp_min_valid = 10.0
        temp_max_valid = 40.0
        df.loc[(df[nama_kolom_suhu] < temp_min_valid) | (df[nama_kolom_suhu] > temp_max_valid), nama_kolom_suhu] = pd.NA

        # --- Kerangka waktu lengkap ---
        start_date, end_date = df[nama_kolom_timestamp].min(), df[nama_kolom_timestamp].max()
        full_date_range = pd.date_range(start=start_date, end=end_date, freq='h')
        df_master = pd.DataFrame({nama_kolom_timestamp: full_date_range})
        df_complete = pd.merge(df_master, df, on=nama_kolom_timestamp, how='left')

        # --- Imputasi ---
        df_complete['year'] = df_complete[nama_kolom_timestamp].dt.year
        df_complete['month'] = df_complete[nama_kolom_timestamp].dt.month
        df_complete['hour'] = df_complete[nama_kolom_timestamp].dt.hour
        suhu_rerata_pola = df_complete.groupby(['month', 'hour'])[nama_kolom_suhu].transform('mean')
        df_complete[nama_kolom_suhu] = df_complete[nama_kolom_suhu].fillna(suhu_rerata_pola)
        df_complete[nama_kolom_suhu].interpolate(method='linear', limit_direction='both', inplace=True)

        # --- Simpan hasil (hanya kolom yang relevan) ---
        df_hasil_simpan = df_complete[[nama_kolom_timestamp, nama_kolom_suhu]]
        df_hasil_simpan.to_csv(file_output_csv, index=False)
        print(f"Data bersih berhasil disimpan ke file CSV: '{file_output_csv}'")
        df_hasil_simpan.to_parquet(file_output_parquet, index=False)
        print(f"Data bersih berhasil disimpan ke file Parquet: '{file_output_parquet}'")

        # PENTING: Kembalikan df_complete agar kolom 'year' dan 'month' bisa dipakai untuk visualisasi
        return df_complete

    except Exception as e:
        print(f"Terjadi error: {e}")
        return None

# Fungsi untuk visualisasi hasil imputasi suhu per bulan
def visualisasi_imputasi_suhu(
    df_complete,
    df_original,
    nama_kolom_timestamp='timestamp',
    nama_kolom_suhu='suhu',
    plot_month='all'
):
    
    """
    Visualisasi hasil imputasi suhu per bulan.
    """
    def create_plot(df_imputed_filtered, df_original_filtered, title):
        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(18, 7))
        # Plot data hasil imputasi sebagai garis
        ax.plot(df_imputed_filtered[nama_kolom_timestamp], df_imputed_filtered[nama_kolom_suhu], '-', color='blue', label='Suhu Hasil Imputasi', zorder=5)
        # Plot data asli sebagai titik-titik untuk menunjukkan data yang ada
        ax.plot(df_original_filtered[nama_kolom_timestamp], df_original_filtered[nama_kolom_suhu], 'o', color='red', label='Suhu Asli (Sebelum Imputasi)', markersize=4, zorder=10)
        ax.set_title(title, fontsize=16, weight='bold')
        ax.set_xlabel('Tanggal', fontsize=12)
        ax.set_ylabel('Suhu (°C)', fontsize=12)
        ax.legend()
        ax.set_ylim(bottom=10)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    month_names = {1: "Januari", 2: "Februari", 3: "Maret", 4: "April", 5: "Mei", 6: "Juni", 7: "Juli", 8: "Agustus", 9: "September", 10: "Oktober", 11: "November", 12: "Desember"}

    # Pastikan tipe data timestamp benar untuk df_original
    df_original[nama_kolom_timestamp] = pd.to_datetime(df_original[nama_kolom_timestamp])

    if plot_month == 'all':
        unique_year_months = sorted(df_complete.groupby(['year', 'month']).groups.keys())
        for year, month_num in unique_year_months:
            imputed_filtered = df_complete[(df_complete['year'] == year) & (df_complete['month'] == month_num)]
            original_filtered = df_original[
                (df_original[nama_kolom_timestamp].dt.year == year) &
                (df_original[nama_kolom_timestamp].dt.month == month_num)
            ]
            plot_title = f"Verifikasi Imputasi Suhu - {month_names.get(month_num, '')} {year}"
            create_plot(imputed_filtered, original_filtered, plot_title)
    elif isinstance(plot_month, int) and 1 <= plot_month <= 12:
        unique_years_for_month = sorted(df_complete[df_complete['month'] == plot_month]['year'].unique())
        for year in unique_years_for_month:
            imputed_filtered = df_complete[(df_complete['year'] == year) & (df_complete['month'] == plot_month)]
            original_filtered = df_original[
                (df_original[nama_kolom_timestamp].dt.year == year) &
                (df_original[nama_kolom_timestamp].dt.month == plot_month)
            ]
            plot_title = f"Verifikasi Imputasi Suhu - {month_names.get(plot_month, '')} {year}"
            create_plot(imputed_filtered, original_filtered, plot_title)
    else:
        # Pesan error yang lebih informatif
        print(f"Pilihan plot tidak valid: '{plot_month}'. Gunakan 'all' atau angka bulan (misal: 7 untuk Juli).")

