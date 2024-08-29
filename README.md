# Visual Autonomy

This project processes and visualizes program data, focusing on dependencies and associations between various programs, companies, and themes. Data is imported from Google Sheets into a PostgreSQL database, transformed, and then loaded into a Neo4j graph database for visualization.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Environment Variables](#environment-variables)
- [Scripts](#scripts)
  - [Data Formatter](#data-formatter)
  - [Get Data](#get-data)
  - [Import to Neo4j](#import-to-neo4j)
  - [Print Levels](#print-levels)
  - [Process Sankey](#process-sankey)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

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
