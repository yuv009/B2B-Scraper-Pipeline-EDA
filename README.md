B2B Cloud Data Pipeline & EDA
=============================

This project implements a scalable, end-to-end, cloud-native ETL pipeline to scrape B2B business listings from TradeIndia, process the raw data, and make it available for exploratory data analysis. The entire workflow is orchestrated using Apache Airflow and deployed on Microsoft Azure, complete with a web-based UI for triggering new data collection runs.

Project Goal
------------

The primary objective was to overcome the challenges of manual data collection from a large-scale B2B platform with anti-bot measures. The goal was to engineer a fully automated, resilient, and scalable data platform capable of ingesting, processing, and storing over 350,000 product listings for supply chain and market analysis.

Architecture
------------

The pipeline is designed with a modern, cloud-native architecture, separating concerns between orchestration, computation, and storage. It follows a **medallion architecture** for data processing, ensuring a clear data lineage from raw to analysis-ready.

### System Flow

The data flows through the system in a decoupled, event-driven manner:

**Streamlit UI → Airflow REST API → Airflow DAG → Azure Data Lake Storage (Bronze → Silver)**

1.  **Trigger:** A user submits a new category URL via a **Streamlit** web application.
    
2.  **Orchestration:** The Streamlit app makes a REST API call to **Apache Airflow**, triggering a new DAG run with the user's input as a configuration parameter.
    
3.  **Execution:** The Airflow DAG, running in **Docker** containers on an **Azure VM**, executes a sequence of tasks:
    
    *   **Crawl:** Scrapes the initial page for sub-category links.
        
    *   **Scrape:** Hits a hidden API to gather raw product data for each sub-category.
        
    *   **Extract:** Parses, cleans, and consolidates the raw data.
        
4.  **Storage:** All data is persisted in **Azure Data Lake Storage (ADLS Gen2)**:
    
    *   **Bronze Container:** Stores the raw, unprocessed JSON files from the scraper.
        
    *   **Silver Container:** Stores the final, cleaned, and consolidated CSV file, ready for analysis.
        
5.  **Metadata:** Airflow's metadata (logs, task status, etc.) is managed by dedicated, highly available PaaS services: **Azure Database for PostgreSQL** and **Azure Cache for Redis**.
    

### Key Technologies

*   **Cloud Platform:** Microsoft Azure
    
*   **Orchestration:** Apache Airflow 2.x (CeleryExecutor)
    
*   **Containerization:** Docker & Docker Compose
    
*   **Compute:** Azure Virtual Machine (Linux)
    
*   **Storage:** Azure Data Lake Storage (ADLS) Gen2
    
*   **Databases:** Azure Database for PostgreSQL, Azure Cache for Redis
    
*   **Frontend:** Streamlit
    
*   **Core Libraries:** Pandas, Requests, BeautifulSoup, Azure SDK for Python
    

Project Structure
-----------------

The project is organized into two main components: the Airflow DAG (the "manager") and the worker scripts (the "application logic").

*   B2B\_pipeline.py: The **Airflow DAG file**. Defines the tasks and their dependencies.
    
*   main.py: The main **worker script** called by the DAG. It contains the high-level logic for each task.
    
*   crawler.py: Module for crawling web pages to discover sub-category links.
    
*   scraper.py: Module for scraping the hidden API to gather raw JSON data. Resilient with retry and stagnation-detection logic.
    
*   feature\_extraction\_script.py: Module for parsing the raw JSON and transforming it into a structured format.
    
*   adls\_helper.py: A crucial helper module that abstracts all interactions with Azure Data Lake Storage, providing functions for uploading, downloading, and listing files.
    
*   streamlit\_app.py: The user-facing web application for triggering pipeline runs.
    

Setup & Deployment
------------------

To replicate this environment, you will need an Azure subscription and the Azure CLI.

1.  **Provision Infrastructure:**
    
    *   Create an Azure Resource Group.
        
    *   Provision the required resources: an Azure VM, Azure Data Lake Storage, Azure Database for PostgreSQL, and Azure Cache for Redis.
        
2.  **Configure Networking:**
    
    *   Set up a Network Security Group for the VM to allow SSH (port 22) and Airflow UI (port 8080) access.
        
    *   Configure the firewall rules on the PostgreSQL and Redis instances to allow connections from the VM's public IP address.
        
3.  **Set Up the VM:**
    
    *   SSH into the VM and install Docker and Docker Compose.
        
    *   Download the official Airflow docker-compose.yml file.
        
    *   Update the .yml file with your Azure database and Redis credentials and add the volume mapping for your project directory.
        
4.  **Deploy the Code:**
    
    *   Clone this repository into a project folder on the VM (e.g., /home/user/airflow-project).
        
    *   Place the B2B\_pipeline.py DAG file into the dags folder that is mapped by Docker Compose.
        
    *   Install the required Python libraries on the VM: pip install -r requirements.txt.
        
5.  **Grant Permissions:**
    
    *   Enable a system-assigned Managed Identity for the VM.
        
    *   Grant this identity the "Storage Blob Data Contributor" role on your ADLS account to allow the scripts to read and write data securely.
        
6.  **Launch Airflow:**
    
    *   From the directory containing your docker-compose.yml, run docker-compose up -d.
        

Running the Pipeline
--------------------

The pipeline is designed to be triggered on-demand via the Streamlit UI or the Airflow UI.

1.  **Start the Streamlit App:**
    
    *   On the VM, navigate to your project directory.
        
    *   Run streamlit run streamlit\_app.py.
        
2.  **Trigger a Run:**
    
    *   Access the Streamlit UI in your browser.
        
    *   Enter a category name (e.g., automobile) and the corresponding URL.
        
    *   Click "Trigger Pipeline".
        
3.  **Monitor in Airflow:**
    
    *   Go to the Airflow UI (http://:8080).
        
    *   You will see a new run for the B2B\_pipeline DAG. You can monitor the progress of the crawl, scrape, and extract tasks in real-time.
        

Exploratory Data Analysis (EDA) & Key Insights
----------------------------------------------

The final, cleaned dataset in the Silver layer serves as the source for a detailed exploratory data analysis, performed in the accompanying Analysis.ipynb Jupyter Notebook. The analysis transforms the structured data into actionable insights for supply chain and market strategy, focusing on several key areas:

### 1\. Data Quality & Integrity Assessment

The initial analysis provides a quantitative overview of data completeness, revealing critical quality gaps that inform the reliability of subsequent findings:

*   **Price Sparsity:** A significant lack of price information (over 68% missing) highlights the prevalence of negotiation-based pricing in the B2B market.
    
*   **Incomplete Supplier Profiles:** Key supplier metrics like year\_established and buyer\_feedback\_score are largely incomplete, indicating a systemic issue that biases any analysis of supplier tenure or reputation.
    

### 2\. Geospatial & Market Concentration Analysis

Geospatial analysis was conducted to understand the physical distribution of the supply base:

*   **Domestic Concentration:** The supply base for all major product categories is critically dependent on the industrial hubs of **Maharashtra** and **Gujarat**, indicating both an opportunity for sourcing efficiency and a significant risk of regional disruption.
    
*   **International Specialization:** International suppliers exhibit strong national specialization. Sourcing strategies for industrial chemicals should target **China**, while efforts for electronic components should focus on **Taiwan** and **South Korea**.
    

### 3\. Comparative Market Analysis (Coimbatore Deep Dive)

A specific deep-dive analysis was performed to benchmark the market of a Tier-2 city, Coimbatore, against the top-performing city for each sub-category. From an applied maths perspective, this involved a direct comparative analysis of frequency distributions.

*   **Findings:** The analysis revealed that while Coimbatore has a strong presence in sectors like textile-machinery-parts, it significantly lags behind market leaders in high-volume categories like industrial-chemicals, providing a granular view of regional competitive advantages.
