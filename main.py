import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# ID generators for each dimension table
def id_generator(start=1):
    current = start
    while True:
        yield current
        current += 1

customer_id_gen = id_generator()
date_id_gen = id_generator()
channel_id_gen = id_generator()
account_id_gen = id_generator()
transaction_type_id_gen = id_generator()
location_id_gen = id_generator()
currency_id_gen = id_generator()
investment_type_id_gen = id_generator()
loan_id_gen = id_generator()

# Generate dimensional data
def generate_dim_customer(num_records=100):
    customers = []
    for _ in range(num_records):
        customer = {
            'customer_id': next(customer_id_gen),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'address': fake.address().replace('\n', ','),
            'city': fake.city(),
            'state': fake.state(),
            'postal_code': fake.postalcode(),
            'phone_number': fake.phone_number(),
            'employment_status': random.choice(['Employed', 'Self-Employed', 'Unemployed']),
            'annual_income': round(random.uniform(10000, 150000), 2),  # income in USD
            'credit_score': random.randint(300, 850),  # typical credit score
            'banking_product_usage': random.choice(['Digital Wallet', 'Credit Card', 'Savings Account', 'Personal Loan'])
        }
        customers.append(customer)
    return customers


def generate_dim_date(start_year=2020, end_year=2024):
    date_ls = []
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    delta = timedelta(days=1)

    for i, day in enumerate(range((end_date - start_date).days + 1), start=1):
        day_date = start_date + day * delta
        date_record = {
            'date_id': next(date_id_gen),
            'date': day_date.strftime('%Y-%m-%d'),
            'day': day_date.day,
            'month': day_date.month,
            'year': day_date.year,
            'weekday': day_date.weekday() + 1
        }
        date_ls.append(date_record)
    return date_ls


def generate_dim_channel():
    channels = []
    channel_names = ['Online', 'Mobile App', 'In-Store', 'ATM', 'Telephone']
    for name in channel_names:
        channel = {
            'channel_id': next(channel_id_gen),
            'channel_name': name
        }
        channels.append(channel)
    return channels


def generate_dim_account(num_records=100, customer_ids=None):
    accounts = []
    for _ in range(num_records):
        account = {
            'account_id': next(account_id_gen),
            'customer_id': random.choice(customer_ids),
            'account_number': fake.unique.random_int(min=1000000000, max=9999999999),
            'account_type': random.choice(['Digital Wallet', 'Credit Card', 'Savings Account', 'Personal Loan']),
            'account_balance': round(random.uniform(0, 1000000), 2),
            'credit_score': random.randint(300, 850)
        }
        accounts.append(account)
    return accounts


def generate_dim_transaction_type():
    transaction_types = [
        {
            'transaction_type_id': next(transaction_type_id_gen),
            'transaction_type_name': 'Deposit'
        },
        {
            'transaction_type_id': next(transaction_type_id_gen),
            'transaction_type_name': 'Withdrawal'
        },
        {
            'transaction_type_id': next(transaction_type_id_gen),
            'transaction_type_name': 'Transfer'
        },
        {
            'transaction_type_id': next(transaction_type_id_gen),
            'transaction_type_name': 'Bill Payment'
        },
        {
            'transaction_type_id': next(transaction_type_id_gen),
            'transaction_type_name': 'Loan Payment'
        },
    ]
    return transaction_types


def generate_dim_location(num_records=50):
    locations = []
    for _ in range(num_records):
        location = {
            'location_id': next(location_id_gen),
            'city': fake.city(),
            'state': fake.state(),
            'country': fake.country(),
            'postal_code': fake.postcode()
        }
        locations.append(location)
    return locations


def generate_dim_currency():
    return [
        {
            'currency_id': next(currency_id_gen),
            'currency_code': 'USD',
            'currency_name': 'US Dollar'
        },
        {
            'currency_id': next(currency_id_gen),
            'currency_code': 'EUR',
            'currency_name': 'Euro'
        },
        {
            'currency_id': next(currency_id_gen),
            'currency_code': 'JPY',
            'currency_name': 'Japanese Yen'
        },
    ]


# Define the generate_dim_loan function to generate loan dimension data
def generate_dim_loan(num_records=50):
    loans = []
    for _ in range(num_records):
        loan = {
            'loan_id': next(loan_id_gen),
            'loan_type': random.choice(['Mortgage', 'Personal', 'Auto', 'Student']),
            'loan_amount': round(random.uniform(5000, 500000), 2),
            'interest_rate': round(random.uniform(1.5, 10.0), 2)
        }
        loans.append(loan)
    return loans


def generate_fact_transaction(num_records, accounts, ls_dates, transaction_types, channels, locations,
                              currencies):
    transactions = []
    for _ in range(num_records):
        transaction = {
            'transaction_id': fake.unique.random_int(min=1, max=100000),
            'date_id': random.choice(ls_dates)['date_id'],
            'transaction_type_id': random.choice(transaction_types)['transaction_type_id'],
            'account_id': random.choice(accounts)['account_id'],
            'channel_id': random.choice(channels)['channel_id'],
            'location_id': random.choice(locations)['location_id'],
            'currency_id': random.choice(currencies)['currency_id'],
            'transaction_amount': round(random.uniform(1.00, 10000), 2),
            'merchant_name': fake.company(),
            'transaction_category': random.choice(['Digital Payment', 'Online Shopping', 'Bank Transfer', 'Cash Withdrawal']),
            'transaction_status': random.choice(['Pending', 'Completed', 'Failed']),
            'transaction_time': fake.date_this_year()
        }
        transactions.append(transaction)
    return transactions


def generate_fact_loan(num_records, accounts, ls_dates, loans, locations, currencies):
    fact_loans = []
    for _ in range(num_records):
        loan = {
            'loan_fact_id': fake.unique.random_int(min=1, max=100000),
            'date_id': random.choice(ls_dates)['date_id'],
            'loan_id': random.choice(loans)['loan_id'],
            'account_id': random.choice(accounts)['account_id'],
            'location_id': random.choice(locations)['location_id'],
            'currency_id': random.choice(currencies)['currency_id'],
            'loan_amount': round(random.uniform(5000, 500000), 2),
            'loan_duration': random.choice([12, 24, 36, 48, 60]),  # loan duration in months
            'interest_rate': round(random.uniform(3.5, 9.0), 2),
            'loan_status': random.choice(['Pending', 'Approved', 'Rejected']),
            'last_repayment_date': fake.date_this_year()
        }
        fact_loans.append(loan)
    return fact_loans


def generate_fact_customer_interactions(num_records, customers, ls_dates, channels, locations):
    interactions = []
    for _ in range(num_records):
        interaction = {
            'interaction_id': fake.unique.random_int(min=1, max=100000),
            'date_id': random.choice(ls_dates)['date_id'],
            'customer_id': random.choice(customers)['customer_id'],
            'channel_id': random.choice(channels)['channel_id'],
            'location_id': random.choice(locations)['location_id'],
            'interaction_type': random.choice(['Call', 'Email', 'Chat', 'In-Person']),
            'interaction_rating': random.randint(1, 5),
            'feedback': fake.sentence()
        }
        interactions.append(interaction)
    return interactions

# Helper function to write data to CSV
def write_to_csv(data, filename):
    if not data:
        return  # exit if data is empty
    keys = data[0].keys()
    with open(filename, 'w', newline="", encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys, delimiter="|")
        dict_writer.writeheader()
        dict_writer.writerows(data)

# Main function to generate data
def main():
    customers = generate_dim_customer(100)
    ls_dates = generate_dim_date(2020, 2024)
    channels = generate_dim_channel()
    transaction_types = generate_dim_transaction_type()
    locations = generate_dim_location(50)
    currencies = generate_dim_currency()
    accounts = generate_dim_account(100, [customer['customer_id'] for customer in customers])
    loans = generate_dim_loan(50)

    # Write dimension tables to CSV
    write_to_csv(customers, 'dim_customers.csv')
    write_to_csv(ls_dates, 'dim_dates.csv')
    write_to_csv(channels, 'dim_channels.csv')
    write_to_csv(transaction_types, 'dim_transaction_types.csv')
    write_to_csv(locations, 'dim_locations.csv')
    write_to_csv(currencies, 'dim_currencies.csv')
    write_to_csv(accounts, 'dim_accounts.csv')
    write_to_csv(loans, 'dim_loans.csv')

    # Generate fact tables
    transactions = generate_fact_transaction(10000, accounts, ls_dates, transaction_types, channels, locations, currencies)
    fact_loans = generate_fact_loan(10000, accounts, ls_dates, loans, locations, currencies)
    interactions = generate_fact_customer_interactions(10000, customers, ls_dates, channels, locations)

    # Write fact tables to CSV
    write_to_csv(transactions, 'fact_transactions.csv')
    write_to_csv(fact_loans, 'fact_loans.csv')
    write_to_csv(interactions, 'fact_customer_interactions.csv')

if __name__ == "__main__":
    main()
