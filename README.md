# Employee Data Lifecycle - Azure Account Integration

## Overview

The `promotion_pipeline` is an automated pipeline designed to manage employee data related to onboarding, profile updates, and license assignments. It integrates with Microsoft Azure Active Directory (AAD), PostgreSQL, and SendGrid for email notifications. The pipeline facilitates the update of employee profiles, license management, and sends error notifications when needed.

### Key Features

- Fetch employee profiles from a PostgreSQL database.
- Update user profiles in Azure Active Directory (AAD).
- Assign or remove licenses in AAD.
- Send email notifications using SendGrid for errors and successful updates.
- Log all events and errors for tracking.
- Supports multiple environments (e.g., PRODUCTION, TESTING).
- Configurable to process different countries (e.g., US, CANADA).

---

## Requirements

### Software

- Python 3.x
- Apache Spark
- PostgreSQL
- Microsoft Azure Active Directory (AAD)
- SendGrid
- PowerShell (for executing AAD-related commands)

### Libraries

- `requests`
- `json`
- `logging`
- `configparser`
- `pyspark`
- `psycopg2`
- `sendgrid`
- `concurrent.futures`

---
