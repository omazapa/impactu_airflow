

<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

[![Validate and Test DAGs](https://github.com/omazapa/impactu_airflow/actions/workflows/deploy.yml/badge.svg)](https://github.com/omazapa/impactu_airflow/actions/workflows/deploy.yml)
# ImpactU Airflow ETL

Central repository for Apache Airflow DAGs for the Extraction, Transformation, and Loading (ETL) processes of the ImpactU project.

## 🚀 Description
This project orchestrates data collection from various scientific and academic sources, its processing using the [Kahi](https://github.com/colav/Kahi) tool, and its subsequent loading into query systems such as MongoDB and Elasticsearch.

## 📂 Project Structure
The repository is organized by data lifecycle stages:

*   `extract/`: Extraction logic for sources like OpenAlex, ORCID, ROR, etc.
*   `transform/`: Transformation and normalization processes (Kahi).
*   `load/`: Loading scripts to final destinations.
*   `deploys/`: Deployment logic for external services (APIs, databases) via DAGs.
*   `backups/`: Database backup automation via DAGs.
*   `tests/`: Integration and data quality tests.

## 📋 Requirements and Architecture
For details on design principles (Checkpoints, Idempotency, Parallelism), see the [System Requirements](REQUIREMENTS.md) document.

## 🛠 DAG Naming Standard
To maintain consistency in the Airflow interface, we follow this convention:

| Type | Format | Example |
| :--- | :--- | :--- |
| **Extraction** | `extract_{source}` | `extract_openalex` |
| **Transformation** | `transform_{entity}` | `transform_sources` |
| **Loading** | `load_{db}_{env}` | `load_mongodb_production` |
| **Deployment** | `deploy_{service}_{env}` | `deploy_mongodb_production` |
| **Backup** | `backup_{db}_{name}` | `backup_mongodb_kahi` |
| **Tests** | `tests_{service}` | `tests_kahi` |

## ⚙️ Development and Deployment

This repository focuses exclusively on DAG logic and ETL processes. The base infrastructure is provided by the **Chia** repository.

For details on the CI/CD strategy, image building, and environment management, see the document:
👉 **[README_DEVOPS.md](README_DEVOPS.md)**

### Local Workflow
1. Clone the repository.
2. Install dependencies: `pip install -r requirements.txt`.
3. Develop DAGs in the `dags/` folder.
4. Validate integrity: `pytest tests/etl/test_dag_integrity.py`.

---
**Colav - ImpactU**
