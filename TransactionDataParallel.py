import pandas as pd
import random
import datetime
from concurrent.futures import ProcessPoolExecutor

def generate_transactions_portion(start_date, end_date, amount_participants, amount_securities, transactions_per_process, balance_df, starting_linkcode):
    transactions = []
    linkcode = starting_linkcode  # Initialize with the starting value
    
    for _ in range(transactions_per_process):
        linkcode += 1
        Sending_ID = random.randint(1, amount_participants)
        Receiving_ID = random.randint(1, amount_participants)

        while Sending_ID == Receiving_ID:
            Sending_ID = random.randint(1, amount_participants)
            Receiving_ID = random.randint(1, amount_participants)

        monetary_funds_df = balance_df.loc[(balance_df['Part ID'] == Receiving_ID) & (balance_df['Account ID'] == 0)]
        balance_value = monetary_funds_df['Balance'].iloc[0] if not monetary_funds_df.empty else 0
        transaction_value = generate_transaction_value(balance_value)
        
        random_date_1 = random_datetime(start_date, end_date)
        random_date_2 = random_datetime(start_date, end_date)
        Security_number = random.randint(1, amount_securities)
        
        new_transaction = {
            'Time': random_date_1, 
            'Value': transaction_value, 
            'FromParticipantId': Sending_ID,
            'FromAccountId': Security_number, 
            'ToParticipantId': Receiving_ID,
            'ToAccountId': Security_number, 
            'Linkcode': linkcode
        }
        
        new_counter_transaction = {
            'Time': random_date_2, 
            'Value': transaction_value, 
            'FromParticipantId': Receiving_ID,
            'FromAccountId': 0, 
            'ToParticipantId': Sending_ID,
            'ToAccountId': 0, 
            'Linkcode': linkcode
        }
        
        transactions.extend([new_transaction, new_counter_transaction])
    
    return transactions

def generate_transaction_data_parallel(amount_transactions, amount_participants, amount_securities, days_list, balance_df):
    num_processes = 10  # Adjust based on your system capabilities
    transactions_per_process = amount_transactions // num_processes
    
    datetime_list = [datetime.datetime.strptime(day, "%Y-%m-%d") for day in days_list]
    start_date, end_date = min(datetime_list), max(datetime_list)
    
    # Calculate starting linkcodes for each process
    args = [(start_date, end_date, amount_participants, amount_securities, transactions_per_process, balance_df.copy(), i * transactions_per_process + 1) for i in range(num_processes)]
    
    transactions = []
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(generate_transactions_portion, *arg) for arg in args]
        for future in futures:
            transactions.extend(future.result())
    
    transaction_df = pd.DataFrame(transactions)
    transaction_df = transaction_df.sort_values(by='Time').reset_index(drop=True)
    transaction_df['TID'] = range(1, len(transaction_df) + 1)
    
    return transaction_df

def random_datetime(start_date, end_date):
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_date + datetime.timedelta(seconds=random_seconds)

def generate_transaction_value(balance):
    transaction_value = random.uniform(0.005, 0.1) * balance
    transaction_value = round(transaction_value,2)
    return transaction_value