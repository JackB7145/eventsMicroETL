# Events ETL Pipeline

This repository contains an automated **ETL pipeline (Extract, Transform, Load)** that collects event data from multiple sources, cleans and transforms it, and loads it into a **Postgres database**.  

The pipeline is designed to run on **GitHub Actions** every day, keeping the events database up to date for downstream applications.

---

## Overview

### Data Sources
- **SerpApi** → pulls events data for London, Ontario.  
- **Western University Events Feed** → scrapes official campus event listings.  

### Workflow
1. **Scrape** → Collect raw event data from SerpApi and Western University.  
2. **Transform** → Filter out expired or irrelevant events, drop recurring events, and highlight categories such as *wellness, fitness, and career development*.  
3. **Load** → Insert the cleaned dataset into a Postgres database.  

The pipeline is automated via **GitHub Actions**, with jobs passing artifacts between scraping, transformation, and loading stages.

---

## Architecture

```text
 ┌──────────────────────┐
 │   SerpApi Scraper    │
 └──────────┬───────────┘
            │
 ┌──────────▼───────────┐
 │  Western Scraper     │
 └──────────┬───────────┘
            │
 ┌──────────▼───────────┐
 │   Transform Jobs     │  → Clean & filter raw event data
 └──────────┬───────────┘
            │
 ┌──────────▼───────────┐
 │   Postgres DB        │  ← Loaded daily by GitHub Actions
 └──────────────────────┘
