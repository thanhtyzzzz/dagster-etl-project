# Dagster ETL Pipeline Project

A simple data pipeline built with **Dagster** to extract, transform, and load data from a public API.

## Overview

This project demonstrates a basic **ETL pipeline** implemented with Dagster, including:

- **Extract**: Fetch data from the JSONPlaceholder API (users and posts)
- **Transform**: Clean and enrich the raw data
- **Load**: Persist data to CSV files and a SQLite database
- **Analytics**: Generate summary and aggregated reports

## Quick Start

### 1. Setup

```bash
python -m venv venv

# Linux / Mac
source venv/bin/activate

# Windows
venv\Scripts\activate

pip install -r requirements.txt
mkdir data



### **2. Run Pipeline**
dagster dev -m dagster_etl


**### Pipeline Flow**

Extract → Transform → Load → Analytics



### Notes for Corporate Networks (SSL)

In some corporate or restricted networks, SSL certificate verification may fail when accessing external APIs.

For demonstration and local development purposes, this project disables SSL verification in HTTP requests (verify=False).
