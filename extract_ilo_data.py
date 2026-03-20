import pandas as pd
import requests
import io

# =======================================================
# PHASE 1: DATA EXTRACTION FUNCTIONS
# =======================================================

def fetch_and_clean_ilo_data():
    print("Initiating connection to ILOSTAT direct API...")
    url = "https://rplumber.ilo.org/data/indicator/?id=EMP_TEMP_SEX_OCU_NB_A&format=.csv"
    
    try:
        print("Downloading global occupational data...")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers)
        response.raise_for_status() 
        
        df = pd.read_csv(io.StringIO(response.text))
        print("ILO Data successfully retrieved!")
        
        # Select and rename core columns
        cols_to_keep = ['ref_area', 'time', 'sex', 'classif1', 'obs_value']
        df_clean = df[cols_to_keep].copy()
        df_clean = df_clean[df_clean['sex'] == 'SEX_T'] # Total sex only
        df_clean = df_clean.dropna(subset=['obs_value'])
        
        df_clean = df_clean.rename(columns={
            'ref_area': 'country_code',
            'time': 'year',
            'classif1': 'occupation_code',
            'obs_value': 'employment_count'
        })
        
        return df_clean
    except Exception as e:
        print(f"An error occurred with ILO API: {e}")
        return None

def fetch_world_bank_data():
    print("Fetching Digital Infrastructure data from the World Bank API...")
    url = "https://api.worldbank.org/v2/country/all/indicator/IT.NET.USER.ZS?format=json&per_page=5000"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        records = data[1]
        wb_data = []
        for row in records:
            if row['value'] is not None and row['countryiso3code'] != "":
                wb_data.append({
                    'country_code': row['countryiso3code'],
                    'year': int(row['date']),
                    'internet_penetration_pct': round(row['value'], 2)
                })
                
        df_wb = pd.DataFrame(wb_data)
        print("World Bank data successfully retrieved!")
        return df_wb
    except Exception as e:
        print(f"An error occurred with the World Bank API: {e}")
        return None

# =======================================================
# PHASE 2: CALCULATION LOGIC
# =======================================================

def calculate_ai_index(df):
    print("Calculating AI Exposure Index based on labor market structure...")
    
    # AI Risk Weights (ISCO-08 Categories)
    ai_risk_scores = {
        'OCU_ISCO08_1': 0.70, 'OCU_ISCO08_2': 0.85, 'OCU_ISCO08_3': 0.75, 
        'OCU_ISCO08_4': 0.90, 'OCU_ISCO08_5': 0.40, 'OCU_ISCO08_6': 0.05, 
        'OCU_ISCO08_7': 0.20, 'OCU_ISCO08_8': 0.30, 'OCU_ISCO08_9': 0.10, 
        'OCU_ISCO08_0': 0.10  
    }
    
    df['ai_risk_weight'] = df['occupation_code'].map(ai_risk_scores)
    df = df.dropna(subset=['ai_risk_weight'])
    
    # Calculate share and weighted exposure
    country_totals = df.groupby(['country_code', 'year'])['employment_count'].sum().reset_index()
    country_totals = country_totals.rename(columns={'employment_count': 'total_workforce'})
    
    df = pd.merge(df, country_totals, on=['country_code', 'year'])
    df['workforce_share'] = df['employment_count'] / df['total_workforce']
    df['weighted_ai_exposure'] = df['workforce_share'] * df['ai_risk_weight']
    
    # Aggregate to final Index
    final_index = df.groupby(['country_code', 'year'])['weighted_ai_exposure'].sum().reset_index()
    final_index['ai_vulnerability_index'] = (final_index['weighted_ai_exposure'] * 100).round(2)
    
    return final_index

# =======================================================
# PHASE 3: THE FINAL ETL PIPELINE (EXECUTION)
# =======================================================

print("\n--- STARTING GLOBAL AI LABOR INDEX PIPELINE ---")

# 1. Get Labor Data
df_occupations = fetch_and_clean_ilo_data()

if df_occupations is not None:
    # 2. Process AI Index
    df_ai_scores = calculate_ai_index(df_occupations)
    
    # 3. Get World Bank Data
    df_wb_infra = fetch_world_bank_data()
    
    if df_wb_infra is not None:
        print("Merging datasets and applying 'Latest Year' filter...")
        
        # Merge on country and year
        master_dataset = pd.merge(df_ai_scores, df_wb_infra, on=['country_code', 'year'], how='inner')
        
        # --- THE LATEST YEAR FILTER (Option B) ---
        # Sort by country and year (descending), then keep the first occurrence of each country
        master_latest = master_dataset.sort_values(by=['country_code', 'year'], ascending=[True, False])
        master_latest = master_latest.drop_duplicates(subset=['country_code'], keep='first').copy()
        
        # Sort by high vulnerability for the preview
        master_latest = master_latest.sort_values(by='ai_vulnerability_index', ascending=False)
        
        print(f"\n📊 FINAL MASTER DATASET PREVIEW (Total Countries: {len(master_latest)}):")
        print(master_latest[['country_code', 'year', 'ai_vulnerability_index', 'internet_penetration_pct']].head(15).to_string(index=False))
        
        # 4. EXPORT TO CSV
        output_name = "Global_AI_Labor_Index_Latest.csv"
        master_latest.to_csv(output_name, index=False)
        print(f"\n✅ SUCCESS! Master file saved as: {output_name}")