# Nashville Housing Data Pipeline

This project demonstrates a simple data engineering pipeline that loads raw Nashville housing CSV data into a PostgreSQL database using Docker and Python.

## Project Overview
- Containerized stack with Docker Compose (Postgres, pgAdmin, and a Python ingester).
- Python script that reads the CSV, creates the table if it does not exist, and appends new rows.
- Postgres as the storage layer, accessible through pgAdmin or SQL clients such as DBeaver.
- Environment variables managed with a `.env` file for reproducibility.

## Project Structure
