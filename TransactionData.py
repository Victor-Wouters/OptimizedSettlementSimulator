import pandas as pd
import datetime
import random

def generate_transaction_data(amount_transactions, amount_participants, amount_securities, days_list, balance_df):

    transaction_df = pd.DataFrame(columns=[ 'TID', 'Time', 'Value', 'FromParticipantId','FromAccountId', 'ToParticipantId','ToAccountId','Linkcode'])
    linkcode = 0
    datetime_list = [datetime.datetime.strptime(day, "%Y-%m-%d") for day in days_list]
    datetime_list = sorted(datetime_list)
    start_date = datetime_list[0]
    end_date = datetime_list[-1]

    #Fill in dataframe
    for loop_number in range(amount_transactions):
        linkcode +=1
        #transaction_value = random.randint(min_transaction_value,max_transaction_value)
        Sending_ID = random.randint(1,amount_participants)
        Receiving_ID = random.randint(1,amount_participants)

        while(Sending_ID == Receiving_ID):
            Sending_ID = random.randint(1,amount_participants)
            Receiving_ID = random.randint(1,amount_participants)
        
        monetary_funds_df = balance_df.loc[(balance_df['Part ID'] == Receiving_ID) & (balance_df['Account ID'] == 0)]
        if not monetary_funds_df.empty:
            balance_value = monetary_funds_df['Balance'].iloc[0]
        else:
            print("No record found for the given part_id and account_id.")
        transaction_value = generate_transaction_value(balance_value)
        # Securities transaction
        random_date_1 = random_datetime(start_date, end_date)
        Security_number = random.randint(1,amount_securities)
        
        new_transaction = pd.DataFrame({'Time': random_date_1, 'Value': transaction_value, 'FromParticipantId': Sending_ID,'FromAccountId': Security_number, 'ToParticipantId': Receiving_ID,'ToAccountId': Security_number, 'Linkcode': linkcode}, index=[0])

        # Corresponding money transaction
        random_date_2 = random_datetime(start_date, end_date)
        new_counter_transaction = pd.DataFrame({'Time': random_date_2, 'Value': transaction_value, 'FromParticipantId': Receiving_ID,'FromAccountId': 0, 'ToParticipantId': Sending_ID,'ToAccountId': 0, 'Linkcode': linkcode}, index=[0])

        # Add transactions to dataframe
        transaction_df = pd.concat([transaction_df, new_transaction], ignore_index=True)
        transaction_df = pd.concat([transaction_df, new_counter_transaction], ignore_index=True)

    transaction_df = transaction_df.sort_values(by='Time')

    id_list = [] 
    for i in range(1,(len(transaction_df)+1)):
        id_list.append(i)
    transaction_df['TID'] = id_list

    return transaction_df

def random_datetime(start_date, end_date):
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_date + datetime.timedelta(seconds=random_seconds)

def generate_transaction_value(balance):
    transaction_value = random.uniform(0.005, 0.1) * balance
    transaction_value = round(transaction_value,2)
    return transaction_value