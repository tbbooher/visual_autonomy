# Visual Autonomy

This project processes and visualizes program data, focusing on dependencies and associations between various programs, companies, and themes. Data is imported from Google Sheets into a PostgreSQL database, transformed, and then loaded into a Neo4j graph database for visualization.

## Overview

The project consists of multiple Python scripts that handle different parts of the ETL (Extract, Transform, Load) process:

1. Extract data from Google Sheets.
2. Transform and clean the data.
3. Load the data into a PostgreSQL database.
4. Visualize relationships using Neo4j.

## Setup

### Prerequisites

- Python 3.x
- PostgreSQL
- Neo4j
- Google API credentials
- Git

### Install Required Python Packages

You can install the necessary Python packages using `pip`:

```bash
pip install -r requirements.txt

### Database Setup

Make sure to have this view
```
CREATE VIEW program_company_value AS
SELECT
    ap.id AS program_id,
    pc.company_id,
    ap.total_funding_m / ap.num_companies AS program_value
FROM
    all_programs ap
JOIN
    program_company pc ON ap.id = pc.program_id;
```
