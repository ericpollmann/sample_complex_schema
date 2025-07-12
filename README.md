# Banking Database Challenge

A complex SQLite banking database with embedded edge cases designed for testing LLM agents' ability to discover anomalies through natural language queries.

## Overview

This project creates a realistic banking system database containing:
- 500 customers with varied profiles
- Multiple bank accounts per customer
- 5000+ financial transactions
- Loan and payment records
- Customer service chat history
- Hidden anomalies and suspicious patterns

## Database Schema

The database includes these interconnected tables:
- **banks**: Financial institutions
- **customers**: Customer profiles with risk ratings
- **users**: Online banking credentials
- **accounts**: Bank accounts (checking, savings, etc.)
- **account_customers**: Joint account relationships
- **transactions**: Financial transaction history
- **loans**: Active and defaulted loans
- **payments**: Loan payment records
- **chat_history**: Customer service interactions

## Setup

```bash
python create_banking_schema.py
```

This generates:
- `banking_system.db`: SQLite database file
- `challenge_questions.txt`: Test scenarios for anomaly detection

## Challenge Scenarios

The database contains three main anomaly patterns:

1. **Suspicious Transaction Patterns**: Structured cash withdrawals below reporting thresholds
2. **Account Relationship Anomalies**: Potential identity theft or family fraud indicators
3. **Loan Payment Discrepancies**: Reversed payments and correlated account issues

## Usage

Connect to the database using any SQLite client to explore the data and uncover hidden patterns. The challenge questions guide discovery of specific anomalies embedded in the dataset.