import TransactionData
import ParticipantData
import pandas as pd
import random
import arrivals
import argparse
import sys



def generate_data(amount_transactions, amount_participants, amount_securities, min_balance_value, max_balance_value):

    '''
    parser = argparse.ArgumentParser(description='Settlement simulator input data generator')
    parser.add_argument('-i', "--input", help='ID of the parameters output file')
    args = parser.parse_args()

    if args.input is None:
        sys.exit("Please provide ID of the parameters output file! See --help")
    '''
    #Initializations
    days_list = ["2024-03-01","2024-03-02"] 

    # parameters arrivals
    transactions = 2*amount_transactions
    arrival_factor_before_10 = 80
    arrival_factor_after_4 = 80   
    arrival_factor_closed=5
    arrival_factor_day=40
    start_year,start_month,start_day=2024,1,1
    end_year,end_month,end_day=2024,1,1

    #Log input parameters
    parameters_dataframe = pd.DataFrame({ 'amount transactions': amount_transactions,'amount participants': amount_participants, 'amount securities': amount_securities, 'min balance value': min_balance_value, 'max balance value':max_balance_value}, index=[0])  
    #parameters_dataframe.to_csv(f"InputParameters{args.input}.csv", index=False, sep=';')
    #parameters_dataframe.to_csv("InputParameters.csv", index=False, sep=';')
    #Generate participants
    balance_df = ParticipantData.generate_participant_data_modified(amount_participants, amount_securities, min_balance_value, max_balance_value)
    #Generate transactions
    transaction_df = TransactionData.generate_transaction_data(amount_transactions, amount_participants, amount_securities, days_list, balance_df)
    #print(transaction_df)
    
    arrivals_list=arrivals.simulate_arrivals(transactions, start_year,start_month,start_day,end_year,end_month,end_day, arrival_factor_before_10, arrival_factor_after_4,arrival_factor_closed,arrival_factor_day )
    transaction_df['Time'] = arrivals_list
    #transaction_arrivals_df = pd.concat([transaction_df.drop("Time", axis=1), arrivals_df], axis=1, ignore_index=True)
    #transaction_arrivals_df.columns = ['TID', 'Value', 'FromParticipantId', 'FromAccountId', 'ToParticipantId', 'ToAccountId', 'Linkcode', 'ArrivalTimes']
    
    

    #Export  as CSV
    transaction_df.to_csv("InputData\\TRANSACTION1.csv", index=False, sep=';')
    balance_df.to_csv("InputData\\PARTICIPANTS1.csv", index=False, sep=';')
    #transaction_arrivals_df.to_csv("TRANSACTION_ARRIVALS.csv", index=False, sep=';')

