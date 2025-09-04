import os
import crawler
import scraper
import feature_extraction_script as fe_script
import pandas as pd
import json

# --- Configuration ---
# The main directory for all raw data (Bronze layer)
BRONZE_DATA_PATH = "Assignment\Data\Bronze"
# The main directory for all processed data (Silver layer)
SILVER_DATA_PATH = "Assignment\Data\Silver"
# The name of the final, consolidated CSV file
CONSOLIDATED_SILVER_FILE = os.path.join(SILVER_DATA_PATH, "data_raw.csv")

# YOU CAN ADD YOUR CUSTOM CATEGORY LINKS
TARGET_CATEGORIES = {
    "chemicals": "https://www.tradeindia.com/seller/chemicals/",
    "machinery": "https://www.tradeindia.com/seller/machinery/",
    "industrial-supplies": "https://www.tradeindia.com/seller/industrial-supplies/",
    "electronics-electrical-supplies": "https://www.tradeindia.com/seller/electronics-electrical-supplies/",
    "textiles-fabrics": "https://www.tradeindia.com/seller/textiles-fabrics/"
}

# --- Main Pipeline Execution ---
if __name__ == "__main__":
    print("="*50)
    print("🚀 STARTING DATA PIPELINE 🚀")
    print("="*50)

    # Ensure the silver data directory exists
    os.makedirs(SILVER_DATA_PATH, exist_ok=True)
    # --- Step 1 & 2: Crawl and Scrape data for each category ---
    for category_name, category_url in TARGET_CATEGORIES.items():
        print(f"\n{'='*20} Processing Category: {category_name.upper()} {'='*20}")
        
        # Define the specific output directory for this category's raw data
        category_bronze_path = os.path.join(BRONZE_DATA_PATH, category_name)
        os.makedirs(category_bronze_path, exist_ok=True)
        
        # Define the path for the sub-category links CSV file
        links_csv_path = os.path.join(category_bronze_path, f"{category_name}_links.csv")

        # Run the crawler as a module
        print("\n[PHASE 1/3] Running Crawler...")
        if not crawler.run_crawler_logic(category_url, links_csv_path):
            print(f"[SKIPPING] Failed to crawl '{category_name}'. Moving to next category.")
            continue

        # Run the scraper, which saves raw JSON and returns the data
        print("\n[PHASE 2/3] Running Scraper...")
        all_raw_data_for_category, success_flag = scraper.run_scraper_logic(links_csv_path, category_bronze_path)
            
        if not success_flag or not all_raw_data_for_category:
            print(f"[SKIPPING] Scraper failed for '{category_name}'. Moving to next category.")
            continue
            
        print(f"\n[SUCCESS] Raw data for '{category_name}' saved to: {category_bronze_path}")

        # --- Step 3: Run Feature Extraction and append to the silver layer ---
        print("\n[PHASE 3/3] Running Feature Extraction and Consolidation...")
        
        all_clean_products = []
        
        # Manually loop through the bronze layer files to perform the feature extraction
        for filename in os.listdir(category_bronze_path):
            if filename.endswith(".json"):
                # Get the sub-category name from the filename.
                sub_category = filename.replace('.json', '')
                print(f"--- Processing file: {filename} ---")
                
                file_path = os.path.join(category_bronze_path, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    try:
                        raw_data = json.load(f)
                    except json.JSONDecodeError as e:
                        print(f"[ERROR] Could not decode JSON from {filename}: {e}")
                        continue
                
                for product_item in raw_data:
                    # Only process items that are actual product records.
                    if product_item.get('is_product_record') == 1:
                        clean_product = fe_script.parse_product_data(product_item, sub_category, category_name)
                        all_clean_products.append(clean_product)


        if all_clean_products:
            new_df = pd.DataFrame(all_clean_products)
            
            # Append the new data to the existing file or create a new one
            if os.path.exists(CONSOLIDATED_SILVER_FILE):
                existing_df = pd.read_csv(CONSOLIDATED_SILVER_FILE)
                consolidated_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                consolidated_df = new_df

            # Save the consolidated DataFrame to the CSV file
            consolidated_df.to_csv(CONSOLIDATED_SILVER_FILE, index=False)
            print(f"\n[SUCCESS] Extracted features appended to: {CONSOLIDATED_SILVER_FILE}")
            print(f"Total rows in consolidated file: {len(consolidated_df)}")
        else:
            print(f"[WARNING] No features extracted for '{category_name}'.")
        

    print("\n" + "="*50)
    print("✅ PIPELINE FINISHED ✅")
    print("="*50)