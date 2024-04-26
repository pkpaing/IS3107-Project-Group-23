# IS3107_Project_Group_23
# Project Overview

This document outlines the system architecture for our project, which is hosted on a Kamatera cloud server. We utilize Docker containers to ensure a modular, scalable, and efficient deployment.

## System Components

### PostgreSQL Database

Our data warehousing solution is managed through a PostgreSQL database. This provides robust data storage and management capabilities essential for handling our project's data needs.

- **pgAdmin4 Access**: [Click here to access pgAdmin4](http://103.195.4.105/pgadmin4)
  - **Username**: e0727237@u.nus.edu
  - **Password**: Is3107@1234567

### FastAPI Application

We employ a FastAPI application to handle backend operations, providing a fast, reliable, and scalable API.

- **API Documentation**: [View FastAPI Documentation](http://103.195.4.105:8000/docs)

### Apache Airflow (not deployed due to insufficient resources on server)

Apache Airflow is used for workflow orchestration within our project, allowing for scheduled and automated data processing tasks.

### Streamlit Application

For data visualization and machine learning modeling, we utilize a Streamlit application. This allows users to interactively explore data and models.

- **Streamlit App**: [Access the Streamlit App](http://103.195.4.105:8501/)

## Security Notice

Please ensure secure handling of the credentials provided above. Avoid sharing sensitive information publicly or in insecure environments.
