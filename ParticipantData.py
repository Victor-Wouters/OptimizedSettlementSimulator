import pandas as pd
import random

def generate_participant_data(amount_participants, amount_securities, min_balance_value, max_balance_value):

    balance_df = pd.DataFrame(columns=[ 'Part ID','Account ID', 'Balance', 'Credit limit'])

    for participant in range(1,amount_participants+1):
        for account_type in range(amount_securities+1):
            balance_value = random.randint(min_balance_value,max_balance_value)
            credit_limit =  round(random.uniform(0.25, 1) * balance_value, 2)
            #credit_limit = random.randint(min_balance_value,max_balance_value)
            new_row_balance = pd.DataFrame({ 'Part ID': participant,'Account ID': account_type, 'Balance': balance_value, 'Credit limit': credit_limit}, index=[0])
            balance_df = pd.concat([balance_df, new_row_balance], ignore_index=True)
    
    return balance_df

def generate_participant_data_modified(amount_participants, amount_securities, min_balance_value, max_balance_value):

    balance_df = pd.DataFrame(columns=[ 'Part ID','Account ID', 'Balance', 'Credit limit'])

    for participant in range(1,amount_participants+1):
        total_securities_part = 0
        for account_type in range(amount_securities+1):
            if account_type == 0:
                balance_value = None
                credit_limit =  None
                new_row_balance = pd.DataFrame({ 'Part ID': participant,'Account ID': account_type, 'Balance': balance_value, 'Credit limit': credit_limit}, index=[0])
                balance_df = pd.concat([balance_df, new_row_balance], ignore_index=True)
            else: 
                balance_value = random.randint(min_balance_value,max_balance_value)
                total_securities_part += balance_value
                credit_limit =  0
                new_row_balance = pd.DataFrame({ 'Part ID': participant,'Account ID': account_type, 'Balance': balance_value, 'Credit limit': credit_limit}, index=[0])
                balance_df = pd.concat([balance_df, new_row_balance], ignore_index=True)

        part_condition = (balance_df['Part ID'] == participant)
        acc_condition = (balance_df['Account ID'] == 0)
        cash_percentage = random.uniform(0.02, 0.1)
        balance_value = round((cash_percentage/(1-cash_percentage)) * total_securities_part, 2)
        balance_df.loc[part_condition & acc_condition, 'Balance'] = balance_value
        credit_limit =  round(random.uniform(0.25, 1) * balance_value, 2)
        balance_df.loc[part_condition & acc_condition, 'Credit limit'] = credit_limit
    
    return balance_df