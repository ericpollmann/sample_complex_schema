#!/usr/bin/env python3
"""
Complex Banking Database Schema with Embedded Edge Cases
This script creates a SQLite database with realistic banking data and hidden anomalies
for LLM agents to discover through natural language queries.
"""

import sqlite3
import random
import datetime
import json
from faker import Faker
from decimal import Decimal
import hashlib

fake = Faker()
Faker.seed(42)
random.seed(42)

# Reset seeds at the beginning of each run
def reset_seeds():
    Faker.seed(42)
    random.seed(42)

class BankingDatabase:
    def __init__(self, db_path='banking_system.db'):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute("PRAGMA foreign_keys = ON")
        
    def create_schema(self):
        """Create all tables with proper relationships"""
        
        # Banks table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS banks (
            bank_id INTEGER PRIMARY KEY,
            bank_name TEXT NOT NULL,
            swift_code TEXT UNIQUE NOT NULL,
            country TEXT NOT NULL,
            headquarters_city TEXT NOT NULL,
            established_date DATE,
            is_active BOOLEAN DEFAULT 1
        )''')
        
        # Customers table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            date_of_birth DATE NOT NULL,
            ssn_hash TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT NOT NULL,
            address TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            zip_code TEXT NOT NULL,
            customer_since DATE NOT NULL,
            credit_score INTEGER,
            annual_income DECIMAL(10,2),
            employment_status TEXT,
            is_pep BOOLEAN DEFAULT 0,  -- Politically Exposed Person
            risk_rating TEXT DEFAULT 'LOW'  -- LOW, MEDIUM, HIGH
        )''')
        
        # Users table (for online banking)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            last_login TIMESTAMP,
            is_active BOOLEAN DEFAULT 1,
            failed_login_attempts INTEGER DEFAULT 0,
            two_factor_enabled BOOLEAN DEFAULT 0,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )''')
        
        # Accounts table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
            account_id INTEGER PRIMARY KEY,
            account_number TEXT UNIQUE NOT NULL,
            bank_id INTEGER NOT NULL,
            account_type TEXT NOT NULL,  -- CHECKING, SAVINGS, MONEY_MARKET, CD
            balance DECIMAL(15,2) NOT NULL,
            currency TEXT DEFAULT 'USD',
            opened_date DATE NOT NULL,
            closed_date DATE,
            status TEXT DEFAULT 'ACTIVE',  -- ACTIVE, FROZEN, CLOSED
            interest_rate DECIMAL(5,4) DEFAULT 0,
            overdraft_limit DECIMAL(10,2) DEFAULT 0,
            FOREIGN KEY (bank_id) REFERENCES banks(bank_id)
        )''')
        
        # Account_Customers junction table (joint accounts)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS account_customers (
            account_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            relationship_type TEXT NOT NULL,  -- PRIMARY, JOINT, BENEFICIARY
            added_date DATE NOT NULL,
            PRIMARY KEY (account_id, customer_id),
            FOREIGN KEY (account_id) REFERENCES accounts(account_id),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )''')
        
        # Transactions table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL,
            transaction_type TEXT NOT NULL,  -- DEPOSIT, WITHDRAWAL, TRANSFER, FEE, INTEREST
            amount DECIMAL(15,2) NOT NULL,
            balance_after DECIMAL(15,2) NOT NULL,
            transaction_date TIMESTAMP NOT NULL,
            description TEXT,
            category TEXT,  -- GROCERIES, UTILITIES, ENTERTAINMENT, etc.
            merchant_name TEXT,
            reference_number TEXT UNIQUE,
            related_transaction_id INTEGER,  -- For transfers
            location TEXT,
            ip_address TEXT,
            device_id TEXT,
            is_flagged BOOLEAN DEFAULT 0,
            FOREIGN KEY (account_id) REFERENCES accounts(account_id),
            FOREIGN KEY (related_transaction_id) REFERENCES transactions(transaction_id)
        )''')
        
        # Loans table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS loans (
            loan_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            bank_id INTEGER NOT NULL,
            loan_type TEXT NOT NULL,  -- PERSONAL, MORTGAGE, AUTO, STUDENT
            principal_amount DECIMAL(15,2) NOT NULL,
            interest_rate DECIMAL(5,4) NOT NULL,
            term_months INTEGER NOT NULL,
            monthly_payment DECIMAL(10,2) NOT NULL,
            remaining_balance DECIMAL(15,2) NOT NULL,
            origination_date DATE NOT NULL,
            maturity_date DATE NOT NULL,
            status TEXT DEFAULT 'ACTIVE',  -- ACTIVE, PAID_OFF, DEFAULTED, RESTRUCTURED
            collateral_value DECIMAL(15,2),
            collateral_type TEXT,
            delinquency_days INTEGER DEFAULT 0,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (bank_id) REFERENCES banks(bank_id)
        )''')
        
        # Payments table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            payment_id INTEGER PRIMARY KEY,
            loan_id INTEGER NOT NULL,
            payment_date DATE NOT NULL,
            scheduled_date DATE NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            principal_paid DECIMAL(10,2) NOT NULL,
            interest_paid DECIMAL(10,2) NOT NULL,
            late_fee DECIMAL(10,2) DEFAULT 0,
            payment_method TEXT,  -- AUTO_DEBIT, ONLINE, CHECK, CASH
            transaction_id INTEGER,
            is_reversed BOOLEAN DEFAULT 0,
            reversal_reason TEXT,
            FOREIGN KEY (loan_id) REFERENCES loans(loan_id),
            FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
        )''')
        
        # Chat History table (customer service interactions)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS chat_history (
            chat_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            user_id INTEGER,
            session_start TIMESTAMP NOT NULL,
            session_end TIMESTAMP,
            channel TEXT NOT NULL,  -- WEB, MOBILE, PHONE, BRANCH
            agent_id TEXT,
            topic TEXT,  -- ACCOUNT_INQUIRY, DISPUTE, LOAN, TECHNICAL, COMPLAINT
            sentiment_score DECIMAL(3,2),  -- -1 to 1
            resolution_status TEXT,  -- RESOLVED, ESCALATED, PENDING
            transcript TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )''')
        
        # Create indexes for performance
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_payments_loan ON payments(loan_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_account_customers ON account_customers(customer_id)')
        
        self.conn.commit()
        
    def generate_banks(self):
        """Generate bank data"""
        banks = [
            ('First National Bank', 'FNBAUS33', 'USA', 'New York', '1863-06-03'),
            ('Global Trust Bank', 'GTBKUS44', 'USA', 'San Francisco', '1985-04-15'),
            ('Community Savings Bank', 'CSBKUS66', 'USA', 'Chicago', '1924-11-20'),
            ('Digital First Bank', 'DFBKUS77', 'USA', 'Austin', '2018-01-10'),
            ('Metropolitan Bank & Trust', 'MBTCUS22', 'USA', 'Los Angeles', '1962-07-08'),
        ]
        
        for name, swift, country, city, established in banks:
            self.cursor.execute('''
                INSERT OR IGNORE INTO banks (bank_name, swift_code, country, headquarters_city, established_date)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, swift, country, city, established))
        
        self.conn.commit()
        
    def generate_customers(self, count=500):
        """Generate customer data with some edge cases"""
        customers = []
        
        # Create a new Faker instance with different locale for variety
        Faker.seed(42 + count)
        
        for i in range(count):
            # Edge case 1: Create a family with suspiciously similar details
            if i in [101, 102, 103]:
                last_name = 'Johnson'
                city = 'Miami'
                state = 'FL'
                address = f'{random.randint(100, 105)} Oak Street'
                zip_code = '33101'
            else:
                last_name = fake.last_name()
                city = fake.city()
                state = fake.state_abbr()
                address = fake.street_address()
                zip_code = fake.zipcode()
            
            # Edge case 2: High-risk individual
            if i == 249:  # Customer ID will be 250 (1-indexed)
                is_pep = 1
                risk_rating = 'HIGH'
                annual_income = 15000000.00
            else:
                is_pep = random.choice([0] * 99 + [1])
                risk_rating = random.choice(['LOW'] * 70 + ['MEDIUM'] * 25 + ['HIGH'] * 5)
                annual_income = random.uniform(25000, 250000)
            
            ssn = f'{fake.ssn()}-{i}'
            ssn_hash = hashlib.sha256(ssn.encode()).hexdigest()
            
            first_name = fake.first_name()
            customer = (
                first_name,
                last_name,
                fake.date_of_birth(minimum_age=18, maximum_age=85),
                ssn_hash,
                f'{first_name.lower()}.{last_name.lower()}{i}{random.randint(100, 999)}@{fake.free_email_domain()}',
                fake.phone_number(),
                address,
                city,
                state,
                zip_code,
                fake.date_between(start_date='-10y', end_date='today'),
                random.randint(300, 850),
                annual_income,
                random.choice(['EMPLOYED', 'SELF_EMPLOYED', 'RETIRED', 'STUDENT', 'UNEMPLOYED']),
                is_pep,
                risk_rating
            )
            customers.append(customer)
        
        self.cursor.executemany('''
            INSERT INTO customers (first_name, last_name, date_of_birth, ssn_hash, email, phone,
                                 address, city, state, zip_code, customer_since, credit_score,
                                 annual_income, employment_status, is_pep, risk_rating)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', customers)
        
        self.conn.commit()
        
    def generate_users(self):
        """Generate user accounts for online banking"""
        self.cursor.execute('SELECT customer_id FROM customers')
        customer_ids = [row[0] for row in self.cursor.fetchall()]
        
        users = []
        for customer_id in random.sample(customer_ids, int(len(customer_ids) * 0.8)):
            username = f'user{customer_id}'
            password_hash = hashlib.sha256(f'password{customer_id}'.encode()).hexdigest()
            created_at = fake.date_time_between(start_date='-3y', end_date='now')
            last_login = fake.date_time_between(start_date=created_at, end_date='now')
            
            # Edge case: Compromised account with many failed logins
            if customer_id == 250:
                failed_attempts = 47
                is_active = 0
            else:
                failed_attempts = random.choice([0] * 80 + [1] * 15 + [2] * 4 + [3])
                is_active = 1
            
            user = (
                customer_id,
                username,
                password_hash,
                created_at,
                last_login,
                is_active,
                failed_attempts,
                random.choice([0, 0, 0, 1])
            )
            users.append(user)
        
        self.cursor.executemany('''
            INSERT INTO users (customer_id, username, password_hash, created_at, 
                             last_login, is_active, failed_login_attempts, two_factor_enabled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', users)
        
        self.conn.commit()
        
    def generate_accounts(self):
        """Generate account data"""
        self.cursor.execute('SELECT customer_id FROM customers')
        customer_ids = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute('SELECT bank_id FROM banks')
        bank_ids = [row[0] for row in self.cursor.fetchall()]
        
        accounts = []
        account_customers = []
        
        for i, customer_id in enumerate(customer_ids):
            # Most customers have 1-3 accounts
            num_accounts = random.choices([1, 2, 3, 4], weights=[40, 35, 20, 5])[0]
            
            for j in range(num_accounts):
                account_number = f'{1000000000 + (i * 10) + j}'
                bank_id = random.choice(bank_ids)
                account_type = random.choice(['CHECKING', 'SAVINGS', 'MONEY_MARKET', 'CD'])
                
                # Edge case 3: Account with unusually high balance for income
                if customer_id == 250:
                    balance = 5000000.00 if j == 0 else random.uniform(1000, 50000)
                else:
                    balance = random.uniform(100, 100000)
                
                opened_date = fake.date_between(start_date='-5y', end_date='today')
                
                if account_type == 'SAVINGS':
                    interest_rate = random.uniform(0.01, 0.05)
                elif account_type == 'MONEY_MARKET':
                    interest_rate = random.uniform(0.02, 0.06)
                elif account_type == 'CD':
                    interest_rate = random.uniform(0.03, 0.07)
                else:
                    interest_rate = 0
                
                account = (
                    account_number,
                    bank_id,
                    account_type,
                    balance,
                    'USD',
                    opened_date,
                    None,
                    'ACTIVE',
                    interest_rate,
                    random.choice([0, 500, 1000]) if account_type == 'CHECKING' else 0
                )
                accounts.append(account)
                
        self.cursor.executemany('''
            INSERT INTO accounts (account_number, bank_id, account_type, balance, currency,
                                opened_date, closed_date, status, interest_rate, overdraft_limit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', accounts)
        
        # Link accounts to customers
        self.cursor.execute('SELECT account_id FROM accounts')
        account_ids = [row[0] for row in self.cursor.fetchall()]
        
        # Regular account ownership
        for i, account_id in enumerate(account_ids):
            customer_id = customer_ids[i % len(customer_ids)]
            account_customers.append((
                account_id,
                customer_id,
                'PRIMARY',
                fake.date_between(start_date='-5y', end_date='today')
            ))
        
        # Edge case: Joint accounts for the Johnson family
        johnson_customers = [101, 102, 103]
        joint_account_id = account_ids[150]  # Arbitrary joint account
        for cust_id in johnson_customers:
            account_customers.append((
                joint_account_id,
                cust_id,
                'JOINT',
                fake.date_between(start_date='-2y', end_date='today')
            ))
        
        self.cursor.executemany('''
            INSERT OR IGNORE INTO account_customers (account_id, customer_id, relationship_type, added_date)
            VALUES (?, ?, ?, ?)
        ''', account_customers)
        
        self.conn.commit()
        
    def generate_transactions(self, count=5000):
        """Generate transaction data with embedded patterns"""
        self.cursor.execute('SELECT account_id, balance FROM accounts WHERE status = "ACTIVE"')
        accounts = self.cursor.fetchall()
        
        # Get accounts for customer 250 for suspicious transactions
        self.cursor.execute('''
            SELECT a.account_id 
            FROM accounts a 
            JOIN account_customers ac ON a.account_id = ac.account_id 
            WHERE ac.customer_id = 250
        ''')
        customer_250_accounts = [row[0] for row in self.cursor.fetchall()]
        
        transactions = []
        
        # Categories and merchants
        categories = {
            'GROCERIES': ['Whole Foods', 'Kroger', 'Safeway', 'Trader Joes'],
            'UTILITIES': ['Electric Company', 'Water Department', 'Gas Company', 'Internet Provider'],
            'ENTERTAINMENT': ['Netflix', 'Spotify', 'AMC Theaters', 'Live Nation'],
            'DINING': ['Starbucks', 'McDonalds', 'Chipotle', 'Local Restaurant'],
            'SHOPPING': ['Amazon', 'Target', 'Walmart', 'Best Buy'],
            'TRANSPORTATION': ['Uber', 'Shell Gas', 'Chevron', 'Public Transit'],
            'HEALTHCARE': ['CVS Pharmacy', 'Walgreens', 'Medical Center', 'Dental Office'],
            'EDUCATION': ['University', 'Online Course Platform', 'Bookstore'],
            'TRAVEL': ['Delta Airlines', 'Marriott Hotels', 'Airbnb', 'Expedia'],
            'FINANCIAL': ['ATM Withdrawal', 'Wire Transfer', 'Investment Transfer']
        }
        
        for i in range(count):
            # For suspicious transactions window, prefer customer 250's account
            if i >= 2000 and i <= 2050 and customer_250_accounts:
                # Find the account balance for customer 250's first account
                for acc_id, bal in accounts:
                    if acc_id == customer_250_accounts[0]:
                        account_id, current_balance = acc_id, bal
                        break
            else:
                account_id, current_balance = random.choice(accounts)
            
            # Edge case 1: Suspicious pattern - rapid small transactions
            if i >= 2000 and i <= 2050 and customer_250_accounts and account_id == customer_250_accounts[0]:
                transaction_type = 'WITHDRAWAL'
                amount = random.uniform(495, 499)  # Just under $500 to avoid reporting
                transaction_date = datetime.datetime.now() - datetime.timedelta(days=30, hours=i-2000)
                category = 'FINANCIAL'
                merchant = 'ATM Withdrawal'
                location = random.choice(['Miami, FL', 'New York, NY', 'Los Angeles, CA'])
                is_flagged = 1
            else:
                transaction_type = random.choices(
                    ['DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'FEE', 'INTEREST'],
                    weights=[15, 60, 20, 3, 2]
                )[0]
                
                if transaction_type == 'DEPOSIT':
                    amount = random.uniform(500, 5000)
                    category = 'FINANCIAL'
                    merchant = 'Direct Deposit'
                elif transaction_type == 'WITHDRAWAL':
                    amount = random.uniform(10, 500)
                    category = random.choice(list(categories.keys()))
                    merchant = random.choice(categories[category])
                elif transaction_type == 'TRANSFER':
                    amount = random.uniform(100, 2000)
                    category = 'FINANCIAL'
                    merchant = 'Internal Transfer'
                elif transaction_type == 'FEE':
                    amount = random.choice([5, 10, 15, 25, 35])
                    category = 'FINANCIAL'
                    merchant = 'Bank Fee'
                else:  # INTEREST
                    amount = random.uniform(0.01, 50)
                    category = 'FINANCIAL'
                    merchant = 'Interest Payment'
                
                transaction_date = fake.date_time_between(start_date='-1y', end_date='now')
                location = f'{fake.city()}, {fake.state_abbr()}'
                is_flagged = 0
            
            # Calculate balance after transaction
            if transaction_type in ['DEPOSIT', 'INTEREST']:
                balance_after = float(current_balance) + float(amount)
            else:
                balance_after = float(current_balance) - float(amount)
            
            reference_number = f'TXN{datetime.datetime.now().strftime("%Y%m%d")}{i:06d}'
            
            transaction = (
                account_id,
                transaction_type,
                amount,
                balance_after,
                transaction_date,
                f'{transaction_type} - {merchant}',
                category,
                merchant,
                reference_number,
                None,  # related_transaction_id
                location,
                fake.ipv4(),
                f'DEVICE_{random.randint(1000, 9999)}',
                is_flagged
            )
            transactions.append(transaction)
        
        self.cursor.executemany('''
            INSERT INTO transactions (account_id, transaction_type, amount, balance_after,
                                    transaction_date, description, category, merchant_name,
                                    reference_number, related_transaction_id, location,
                                    ip_address, device_id, is_flagged)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', transactions)
        
        self.conn.commit()
        
    def generate_loans(self):
        """Generate loan data with edge cases"""
        self.cursor.execute('SELECT customer_id, credit_score, annual_income FROM customers')
        customers = self.cursor.fetchall()
        
        self.cursor.execute('SELECT bank_id FROM banks')
        bank_ids = [row[0] for row in self.cursor.fetchall()]
        
        loans = []
        
        for customer_id, credit_score, annual_income in random.sample(customers, 200):
            # Determine loan eligibility and terms based on credit score
            if credit_score >= 750:
                loan_types = ['MORTGAGE', 'AUTO', 'PERSONAL']
                interest_rate_base = 0.03
            elif credit_score >= 650:
                loan_types = ['AUTO', 'PERSONAL']
                interest_rate_base = 0.05
            else:
                loan_types = ['PERSONAL']
                interest_rate_base = 0.08
            
            loan_type = random.choice(loan_types)
            bank_id = random.choice(bank_ids)
            
            if loan_type == 'MORTGAGE':
                principal = random.uniform(150000, 1000000)
                term_months = random.choice([180, 360])  # 15 or 30 years
                interest_rate = interest_rate_base + random.uniform(0, 0.02)
            elif loan_type == 'AUTO':
                principal = random.uniform(15000, 75000)
                term_months = random.choice([36, 48, 60, 72])
                interest_rate = interest_rate_base + random.uniform(0.01, 0.03)
            else:  # PERSONAL
                principal = random.uniform(5000, 50000)
                term_months = random.choice([12, 24, 36, 48, 60])
                interest_rate = interest_rate_base + random.uniform(0.02, 0.05)
            
            # Calculate monthly payment (simplified)
            monthly_rate = interest_rate / 12
            monthly_payment = (principal * monthly_rate * (1 + monthly_rate)**term_months) / ((1 + monthly_rate)**term_months - 1)
            
            origination_date = fake.date_between(start_date='-5y', end_date='-6m')
            maturity_date = origination_date + datetime.timedelta(days=term_months * 30)
            
            # Edge case: Loan with payment irregularities
            if customer_id in [101, 102, 103]:  # Johnson family
                delinquency_days = random.randint(30, 90)
                status = 'DEFAULTED' if delinquency_days > 60 else 'ACTIVE'
                remaining_balance = principal * 0.85  # They've made some payments
            else:
                delinquency_days = random.choices([0, 0, 0, 0, 15, 30], weights=[70, 10, 10, 5, 3, 2])[0]
                status = 'ACTIVE' if delinquency_days < 60 else 'DEFAULTED'
                
                # Calculate remaining balance based on time elapsed
                months_elapsed = (datetime.date.today() - origination_date).days // 30
                if months_elapsed > 0:
                    payments_made = min(months_elapsed, term_months * 0.3)
                    remaining_balance = principal * (1 - payments_made / term_months)
                else:
                    remaining_balance = principal
            
            loan = (
                customer_id,
                bank_id,
                loan_type,
                principal,
                interest_rate,
                term_months,
                monthly_payment,
                remaining_balance,
                origination_date,
                maturity_date,
                status,
                principal * 1.2 if loan_type in ['MORTGAGE', 'AUTO'] else None,
                'REAL_ESTATE' if loan_type == 'MORTGAGE' else 'VEHICLE' if loan_type == 'AUTO' else None,
                delinquency_days
            )
            loans.append(loan)
        
        self.cursor.executemany('''
            INSERT INTO loans (customer_id, bank_id, loan_type, principal_amount, interest_rate,
                             term_months, monthly_payment, remaining_balance, origination_date,
                             maturity_date, status, collateral_value, collateral_type, delinquency_days)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', loans)
        
        self.conn.commit()
        
    def generate_payments(self):
        """Generate payment data with anomalies"""
        self.cursor.execute('''
            SELECT loan_id, monthly_payment, origination_date, status, customer_id 
            FROM loans 
            WHERE status IN ('ACTIVE', 'DEFAULTED')
        ''')
        loans = self.cursor.fetchall()
        
        payments = []
        
        for loan_id, monthly_payment, origination_date, status, customer_id in loans:
            # Calculate number of payments that should have been made
            if isinstance(origination_date, str):
                origination_date = datetime.datetime.strptime(origination_date, '%Y-%m-%d').date()
            months_since_origination = (datetime.date.today() - origination_date).days // 30
            
            for month in range(min(months_since_origination, 36)):  # Limit to 36 payments for performance
                scheduled_date = origination_date + datetime.timedelta(days=month * 30)
                
                # Edge case: Johnson family irregular payments
                if customer_id in [101, 102, 103] and month > 12:
                    if random.random() > 0.5:  # Skip 50% of payments
                        continue
                    payment_date = scheduled_date + datetime.timedelta(days=random.randint(15, 45))
                    amount = monthly_payment * random.uniform(0.5, 0.8)  # Partial payments
                    late_fee = 50.00
                else:
                    # Normal payment behavior
                    days_late = random.choices([0, 0, 0, 3, 7, 15], weights=[70, 10, 10, 5, 3, 2])[0]
                    payment_date = scheduled_date + datetime.timedelta(days=days_late)
                    amount = monthly_payment
                    late_fee = 25.00 if days_late > 5 else 0
                
                # Split payment into principal and interest (simplified)
                interest_paid = amount * 0.3
                principal_paid = amount * 0.7 - late_fee
                
                # Edge case: Payment reversal for investigation
                if loan_id == 150 and month == 15:
                    is_reversed = 1
                    reversal_reason = 'Insufficient funds - payment reversed after 3 days'
                else:
                    is_reversed = 0
                    reversal_reason = None
                
                payment = (
                    loan_id,
                    payment_date,
                    scheduled_date,
                    amount,
                    principal_paid,
                    interest_paid,
                    late_fee,
                    random.choice(['AUTO_DEBIT', 'ONLINE', 'CHECK', 'CASH']),
                    None,  # transaction_id - would link to actual transaction
                    is_reversed,
                    reversal_reason
                )
                payments.append(payment)
        
        self.cursor.executemany('''
            INSERT INTO payments (loan_id, payment_date, scheduled_date, amount,
                                principal_paid, interest_paid, late_fee, payment_method,
                                transaction_id, is_reversed, reversal_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', payments)
        
        self.conn.commit()
        
    def generate_chat_history(self):
        """Generate customer service chat history with sentiment"""
        self.cursor.execute('SELECT customer_id FROM customers')
        customer_ids = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute('SELECT user_id, customer_id FROM users')
        user_mappings = {cust_id: user_id for user_id, cust_id in self.cursor.fetchall()}
        
        chat_topics = {
            'ACCOUNT_INQUIRY': [
                "I need to check my balance",
                "Why was I charged a fee?",
                "Can you explain this transaction?",
                "I want to open a new account"
            ],
            'DISPUTE': [
                "This transaction wasn't mine",
                "I was charged twice",
                "Fraudulent activity on my account",
                "Unauthorized withdrawal"
            ],
            'LOAN': [
                "I need information about my loan",
                "Can I refinance?",
                "Payment didn't go through",
                "Late payment fee dispute"
            ],
            'TECHNICAL': [
                "Can't log into online banking",
                "App keeps crashing",
                "Two-factor authentication issues",
                "Password reset needed"
            ],
            'COMPLAINT': [
                "Terrible service at branch",
                "Been on hold for an hour",
                "Account frozen without notice",
                "Discrimination complaint"
            ]
        }
        
        chats = []
        
        for _ in range(1000):
            customer_id = random.choice(customer_ids)
            user_id = user_mappings.get(customer_id)
            
            topic = random.choice(list(chat_topics.keys()))
            
            # Edge case: High volume of complaints from Johnson family
            if customer_id in [101, 102, 103]:
                topic = random.choices(
                    ['COMPLAINT', 'DISPUTE', 'LOAN', 'ACCOUNT_INQUIRY'],
                    weights=[40, 30, 20, 10]
                )[0]
                sentiment_score = random.uniform(-0.8, -0.3)
                resolution_status = random.choice(['ESCALATED', 'PENDING'])
            else:
                # Normal sentiment distribution
                if topic == 'COMPLAINT':
                    sentiment_score = random.uniform(-0.9, -0.3)
                    resolution_status = random.choice(['ESCALATED', 'RESOLVED', 'PENDING'])
                elif topic == 'DISPUTE':
                    sentiment_score = random.uniform(-0.5, 0.2)
                    resolution_status = random.choice(['RESOLVED', 'PENDING', 'ESCALATED'])
                else:
                    sentiment_score = random.uniform(-0.2, 0.9)
                    resolution_status = random.choice(['RESOLVED', 'RESOLVED', 'PENDING'])
            
            session_start = fake.date_time_between(start_date='-1y', end_date='now')
            session_duration = random.randint(5, 120)  # minutes
            session_end = session_start + datetime.timedelta(minutes=session_duration)
            
            sample_messages = random.choice(chat_topics[topic])
            transcript = json.dumps({
                'messages': [
                    {'role': 'customer', 'content': sample_messages, 'timestamp': str(session_start)},
                    {'role': 'agent', 'content': 'I understand your concern. Let me help you with that.', 'timestamp': str(session_start + datetime.timedelta(minutes=1))},
                    {'role': 'customer', 'content': 'Thank you for looking into this.', 'timestamp': str(session_start + datetime.timedelta(minutes=2))}
                ]
            })
            
            chat = (
                customer_id,
                user_id,
                session_start,
                session_end,
                random.choice(['WEB', 'MOBILE', 'PHONE', 'BRANCH']),
                f'AGENT_{random.randint(100, 999)}',
                topic,
                sentiment_score,
                resolution_status,
                transcript
            )
            chats.append(chat)
        
        self.cursor.executemany('''
            INSERT INTO chat_history (customer_id, user_id, session_start, session_end,
                                    channel, agent_id, topic, sentiment_score,
                                    resolution_status, transcript)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', chats)
        
        self.conn.commit()
        
    def create_challenge_queries(self):
        """Create the challenge questions for finding edge cases"""
        challenges = """
        ========================================
        CHALLENGE QUESTIONS FOR LLM AGENT TESTING
        ========================================
        
        CHALLENGE 1: Suspicious Transaction Patterns
        --------------------------------------------
        Question: "I'm investigating potential structuring activity. Can you find any accounts with 
        multiple cash withdrawals just under the $500 reporting threshold within a short time period? 
        Who owns these accounts and what's unusual about their profile?"
        
        Expected Discovery:
        - Customer ID 250 has 50+ ATM withdrawals between $495-$499 within 30 days
        - This customer is marked as a Politically Exposed Person (PEP) with HIGH risk rating
        - Their account balance is disproportionate to declared income
        - Transactions occurred across multiple geographic locations
        - Account shows 47 failed login attempts and has been deactivated
        
        
        CHALLENGE 2: Account Relationship Anomalies  
        --------------------------------------------
        Question: "We've received reports about possible identity theft involving multiple accounts 
        opened with suspiciously similar information. Can you identify any customer groups that share 
        the same address but might not be legitimately related? What concerning patterns do you see 
        in their loan payment history?"
        
        Expected Discovery:
        - Customers 101, 102, 103 (Johnson family) all share addresses on Oak Street in Miami
        - They have a joint account together (account 151)
        - All three have defaulted loans with 30-90 days delinquency
        - Payment patterns show irregular partial payments after month 12
        - High volume of customer complaints and negative sentiment in chat history
        - This could indicate synthetic identity fraud or family financial distress
        
        
        CHALLENGE 3: Loan Payment Discrepancies
        --------------------------------------------
        Question: "Our accounting team noticed some reversed payments that weren't properly 
        investigated. Can you find any loans where payments were reversed and determine if this 
        correlates with any other suspicious account activity or customer service interactions?"
        
        Expected Discovery:
        - Loan ID 150 had a payment reversal in month 15 due to insufficient funds
        - Need to trace the customer and check their transaction history
        - Look for patterns of disputes or complaints in chat history
        - Check if this customer has other delinquent accounts
        - Investigate if there's a pattern of NSF fees or overdrafts
        
        
        TIPS FOR LLM AGENTS:
        -------------------
        1. Start with broad queries to understand table relationships
        2. Use JOINs effectively to connect customer, account, transaction, and loan data
        3. Look for statistical anomalies (e.g., GROUP BY and HAVING clauses)
        4. Consider time-based patterns and geographic inconsistencies
        5. Cross-reference multiple data points to build a complete picture
        6. Pay attention to risk indicators: failed logins, PEP status, sentiment scores
        """
        
        with open('challenge_questions.txt', 'w') as f:
            f.write(challenges)
        
        print("Challenge questions written to challenge_questions.txt")
        
    def close(self):
        """Close database connection"""
        self.conn.close()


def main():
    print("Creating banking database with embedded edge cases...")
    
    # Remove old database if exists
    import os
    if os.path.exists('banking_system.db'):
        os.remove('banking_system.db')
    
    # Reset seeds for consistency
    reset_seeds()
    
    # Initialize database
    db = BankingDatabase()
    
    # Create schema
    print("Creating database schema...")
    db.create_schema()
    
    # Generate data
    print("Generating banks...")
    db.generate_banks()
    
    print("Generating 500 customers...")
    db.generate_customers(500)
    
    print("Generating user accounts...")
    db.generate_users()
    
    print("Generating accounts...")
    db.generate_accounts()
    
    print("Generating 5000 transactions...")
    db.generate_transactions(5000)
    
    print("Generating loans...")
    db.generate_loans()
    
    print("Generating loan payments...")
    db.generate_payments()
    
    print("Generating 1000 chat history records...")
    db.generate_chat_history()
    
    # Create challenge questions
    db.create_challenge_queries()
    
    # Print summary statistics
    print("\n" + "="*50)
    print("DATABASE SUMMARY")
    print("="*50)
    
    tables = ['banks', 'customers', 'users', 'accounts', 'transactions', 'loans', 'payments', 'chat_history']
    for table in tables:
        db.cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = db.cursor.fetchone()[0]
        print(f"{table.upper()}: {count} records")
    
    db.close()
    print("\nDatabase created successfully: banking_system.db")
    print("Challenge questions saved to: challenge_questions.txt")


if __name__ == "__main__":
    main()