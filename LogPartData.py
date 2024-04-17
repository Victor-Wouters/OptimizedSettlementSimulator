import pandas as pd
import matplotlib.pyplot as plt

def get_partacc_data(participants, transactions_entry):
    # Initialize an empty list to collect the rows
    data = []

    unique_accounts = transactions_entry['FromAccountId'].unique()
    for participant_id, participant_obj in participants.items():
        for account_id in unique_accounts:
            account = participant_obj.get_account(account_id)
            # Append the data as a tuple or list to the 'data' list
            data.append((participant_obj.get_part_id(), account.get_account_id(), account.get_balance()))

    # Convert the collected data into a DataFrame in one step
    balances_status = pd.DataFrame(data, columns=['Participant', 'Account ID', 'Account Balance'])
    
    return balances_status

def balances_history_calculations(balances_history, participants):

    plt.rcParams['font.size'] = 15
     
    balances_history = balances_history.applymap(lambda x: int(x))
    balances_history.to_csv('balanceHistoryCSV\\BalanceHistory.csv', index=False, sep = ';')

    #first_two_columns = balances_history.iloc[:, :2]
    remaining_columns = balances_history.iloc[:, 2:].applymap(lambda x: x if x < 0 else 0)
    #modified_balances_history = pd.concat([first_two_columns, remaining_columns], axis=1)
    total_credit = remaining_columns.sum()
    total_credit_dataframe = total_credit.to_frame().transpose()
    total_credit_dataframe = total_credit_dataframe.abs()
    max_credit = total_credit_dataframe.max().max()
    credit_plot = total_credit_dataframe.iloc[0]
    
    plt.figure(figsize=(15, 8))
    plt.plot(credit_plot.index, credit_plot.values)
    plt.title('Total Credit Over Time')
    plt.xlabel('Time (15-minute intervals)')
    plt.ylabel('Value (€)')
    #plt.xticks(rotation=90)
    x_ticks = credit_plot.index[::24]
    plt.xticks(x_ticks, rotation=90)
    plt.grid(axis='y')
    plt.tight_layout()
    plt.savefig(f'balanceHistoryPNG\\Total_credit.pdf')
    total_credit_dataframe.to_csv('balanceHistoryCSV\\Total_credit.csv', index=False, sep = ';')
    
    dfs = {part_id: group for part_id, group in balances_history.groupby('PartID')}
    for part_id, dataframe in dfs.items():
        credit_limit_row = [None, None] + [-(participants[str(part_id)].get_account('0').get_credit_limit())] * (len(dataframe.columns) - 2)
        dataframe.loc['credit limit'] = credit_limit_row
        dataframe = dataframe.applymap(lambda x: int(x) if pd.notnull(x) and x != '' else '')

        dataframe = dataframe.transpose()
        dataframe = dataframe.reset_index()
        new_header = dataframe.iloc[1]
        dataframe = dataframe.drop(dataframe.index[0])
        dataframe = dataframe.drop(dataframe.index[0])
        dataframe.columns = new_header
        dataframe.rename(columns={'Account ID': 'Time'}, inplace=True)
        dataframe.columns = [*dataframe.columns[:-1], 'Credit limit']
        
        dataframe.to_csv(f'balanceHistoryCSV\\BalanceHistoryPart{part_id}.csv', index=False, sep = ';')


        dataframe.set_index('Time', inplace=True)
        plt.figure(figsize=(15, 8))
        for column in dataframe.columns[:-1]:  # Exclude the last column which is 'credit limit'
            if column == 0:
                plt.plot(dataframe.index, dataframe[column], label=f'Cash account')
            else:
                plt.plot(dataframe.index, dataframe[column], label=f'Security {column} account')

        plt.plot(dataframe.index, dataframe['Credit limit'], label='Credit Limit', linestyle='--')
        plt.xlabel('Time (15-minute intervals)')
        plt.ylabel('Value (€)')
        plt.title(f'Participant {part_id}: Account Values Over Time')
        #plt.xticks(rotation=90)
        x_ticks = dataframe.index[::24]
        plt.xticks(x_ticks, rotation=90)
        plt.grid(axis='y')
        plt.legend()
        #plt.grid(True)
        plt.tight_layout()
        plt.savefig(f'balanceHistoryPNG\\BalanceHistoryPart{part_id}.pdf')
            
    return max_credit