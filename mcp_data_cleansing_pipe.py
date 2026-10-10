import pandas as pd
import re
import time
import json

class ECommerceDataCleansingPipeline:
    """
    Enterprise Ingestion Pipeline utilizing vectorized Pandas logic and compiled
    Regular Expressions to clean raw, unstructured e-commerce product records.
    """
    def __init__(self):
        print("⚙️ [Pipeline Armed] Initializing high-speed vector cleaning arrays...")

    def generate_mock_flipkart_dataset(self) -> pd.DataFrame:
        """
        Generates a high-density, messy raw transaction payload simulation matrix
        to test vector boundaries without needing an external physical CSV file.
        """
        messy_raw_data = {
            "product_id": ["SKU-101", "SKU-102", "SKU-103", "SKU-104", "SKU-105"],
            "product_name": ["Elite Refrigerator Pro", "Smart Cooling Fridge", "Compact Mini-Bar", "Double-Door Supreme", "Industrial Cold Vault"],
            "raw_price": ["₹95,000.00 inclusive of taxes", "₹35,799 Only", "Price: ₹12,499.00", "₹54,000 + GST Extra", "Call for Price / Out of Stock"],
            "raw_rating": ["4.7 Stars out of 5", "  3.9  ", "No Ratings Yet", "4.2", "NaN / Critical Error"],
            "review_count": ["1,245 Reviews", "482", "0 Reviews", "Malformed 99 strings", "0"]
        }
        return pd.DataFrame(messy_raw_data)

    def scrub_and_clean_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Executes parallel regular expression replacements over Pandas columns,
        converting dirty data columns into clean numeric integer metrics.
        """
        start_time = time.perf_counter()
        print("📡 [Ingest Node] Running deep regex parsing arrays across unstructured rows...")

        # 🔑 ANOMALY FIX 1: Vectorized Price Cleansing
        # Removes currency indicators, letters, whitespace, and formatting commas cleanly
        df['cleaned_price'] = df['raw_price'].astype(str).str.replace(r'[^\d.]', '', regex=True)
        # Safely converts empty strings or text anomalies into a numeric 0 baseline numerical matrix
        df['cleaned_price'] = pd.to_numeric(df['cleaned_price'], errors='coerce').fillna(0).astype(float)

        # 🔑 ANOMALY FIX 2: Vectorized Rating Isolation
        # Extracts float decimals out of complex dirty strings cleanly
        df['cleaned_rating'] = df['raw_rating'].astype(str).str.extract(r'(\d+\.\d+|\d+)', expand=False)
        # Sets a fallback baseline matrix of 0.0 for rows missing structural rating markers
        df['cleaned_rating'] = pd.to_numeric(df['cleaned_rating'], errors='coerce').fillna(0.0).astype(float)

        # 🔑 ANOMALY FIX 3: Vectorized Review Integer Extraction
        df['cleaned_reviews'] = df['review_count'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df['cleaned_reviews'] = pd.to_numeric(df['cleaned_reviews'], errors='coerce').fillna(0).astype(int)

        end_time = time.perf_counter()
        self.latency_ms = (end_time - start_time) * 1000
        
        print("-" * 110)
        print("✅ [CLEANSING COMPLETED] Raw unstructured layout blocks parsed with 0% dropped transaction records.")
        print(f"⏱️ Vectorized dataset transformation time: {self.latency_ms:.4f} ms")
        print("-" * 110)
        
        return df

def main():
    print("🚀 Running Sudhir's Sovereign Flipkart Data-Cleansing Pipeline Engine...\n")
    
    # Instantiate the processing pipeline
    pipeline = ECommerceDataCleansingPipeline()
    
    # Step 1: Ingest dirty unstructured mock ledger streams
    raw_dirty_dataframe = pipeline.generate_mock_flipkart_dataset()
    print("📦 [Raw Ingress Block Data View]:")
    print(raw_dirty_dataframe[['product_id', 'raw_price', 'raw_rating', 'review_count']])
    print("\n" + "="*80 + "\n")
    
    # Step 2: Fire vectorized transformations to scrub text metrics
    cleaned_dataframe = pipeline.scrub_and_clean_matrix(raw_dirty_dataframe)
    
    # Step 3: Compile cleaned output view parameters
    print("📦 [Pristine Structured Production Data View]:")
    print(cleaned_dataframe[['product_id', 'product_name', 'cleaned_price', 'cleaned_rating', 'cleaned_reviews']])
    
    # Step 4: Write pristine structured output directly down to local disk space
    OUTPUT_CSV_FILE = "pristine_flipkart_cleaned_vault.csv"
    cleaned_dataframe.to_csv(OUTPUT_CSV_FILE, index=False)
    print(f"\n💾 [Data Persistence Locked] Standardized database exported safely to disk as: '{OUTPUT_CSV_FILE}'")

if __name__ == "__main__":
    main()
    print("\n🎉 Data pipeline lifecycle sync tracking completed with 0% remaining volatile leaks!")
