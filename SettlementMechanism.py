import pandas as pd
import Eventlog
import datetime

def settle(time, end_matching, start_checking_balance, end_checking_balance, queue_2, settled_transactions, participants, event_log, modified_accounts):
    # Add 'Starttime' at the beginning to ensure it exists
    for df in [start_checking_balance]:
        if 'Starttime' not in df.columns:
            df['Starttime'] = pd.NaT

    # Process matched instructions
    if not end_matching.empty:
        end_matching['Starttime'] = time
        start_checking_balance = pd.concat([start_checking_balance, end_matching], ignore_index=True)
        end_matching = end_matching.iloc[0:0]  # Clear efficiently

    # Checking balance and credit activity
    check_time_limit = time - datetime.timedelta(seconds=1)
    mask = start_checking_balance['Starttime'] == check_time_limit
    instructions_ready_for_check = start_checking_balance[mask]
    if not instructions_ready_for_check.empty:
        event_log = Eventlog.Add_to_eventlog(event_log, instructions_ready_for_check['Starttime'], time, instructions_ready_for_check['TID'], activity='Positioning')
        end_checking_balance = pd.concat([end_checking_balance, instructions_ready_for_check], ignore_index=True)
    start_checking_balance = start_checking_balance[~mask]

    # Process checking balance and settlement
    if not end_checking_balance.empty:
        for linkcode in end_checking_balance['Linkcode'].unique():
            instructions_for_processing = end_checking_balance[end_checking_balance['Linkcode'] == linkcode].copy()
            settlement_confirmation = check_balance(True, instructions_for_processing, participants)
            
            if settlement_confirmation:
                instructions_for_processing['Starttime'] = time
                settlement_execution(instructions_for_processing, participants)
                modified_accounts = keep_track_modified_accounts(instructions_for_processing, modified_accounts)
                event_log = Eventlog.Add_to_eventlog(event_log, instructions_for_processing["Starttime"], time, instructions_for_processing['TID'], activity='Settling')
                settled_transactions = pd.concat([settled_transactions, instructions_for_processing], ignore_index=True)
            else:
                queue_2 = pd.concat([queue_2, instructions_for_processing], ignore_index=True)
                event_log = Eventlog.Add_to_eventlog(event_log, time, time, instructions_for_processing['TID'], activity='Waiting in backlog for recycling')
        end_checking_balance = pd.DataFrame()

    return end_matching, start_checking_balance, end_checking_balance, queue_2, settled_transactions, event_log

def check_balance(settlement_confirmation, instructions_for_processing, participants):

    for i in [0,1]:
                from_part = instructions_for_processing['FromParticipantId'].iloc[i]
                from_acc = instructions_for_processing['FromAccountId'].iloc[i]
                transaction_value = instructions_for_processing['Value'].iloc[i]
                current_balance = participants.get(from_part).get_account(from_acc).get_balance()
                credit_limit = participants.get(from_part).get_account(from_acc).get_credit_limit()
                if transaction_value > (current_balance+credit_limit):
                    settlement_confirmation = False
                    
    return settlement_confirmation

def settlement_execution(instructions_for_processing, participants):
     
     for i in [0,1]:
                    from_part = instructions_for_processing['FromParticipantId'].iloc[i]
                    from_acc = instructions_for_processing['FromAccountId'].iloc[i]
                    to_part = instructions_for_processing['ToParticipantId'].iloc[i]
                    to_acc = instructions_for_processing['ToAccountId'].iloc[i]
                    if from_acc != to_acc:
                        raise ValueError("Not the same account type, transfer between different account types!")
                    transaction_value = instructions_for_processing['Value'].iloc[i]
                    participants.get(from_part).get_account(from_acc).edit_balance(-transaction_value)
                    participants.get(to_part).get_account(to_acc).edit_balance(transaction_value)

def keep_track_modified_accounts(instructions_for_processing, modified_accounts):
    for instruction in instructions_for_processing.itertuples(index=False):
        participant_id = instruction.ToParticipantId
        account_id = instruction.ToAccountId

        # If the participant already exists in the dictionary
        if participant_id in modified_accounts:
            # Add the account to the list if it's not already there to avoid duplicates
            if account_id not in modified_accounts[participant_id]:
                modified_accounts[participant_id].append(account_id)
        else:
            # If the participant does not exist, add them with a new list containing the account
            modified_accounts[participant_id] = [account_id]

    
    return modified_accounts

def atomic_retry_settle(time, start_checking_balance, queue_2, settled_transactions, event_log, modified_accounts, cumulative_inserted):
    
    if not queue_2.empty:
        for key, account_list in modified_accounts.items():
            # Iterate through each account for the current participant (key)
            for account_id in account_list:
                # Filter transactions from queue_2 for retry based on current account of the participant
                mask = (queue_2["FromParticipantId"] == key) & (queue_2["FromAccountId"] == account_id)
                first_instruction = queue_2[mask]
                retry_linkcodes = first_instruction['Linkcode'].unique()
                
                for linkcode in retry_linkcodes:
                    mask_linkcode = queue_2["Linkcode"] == linkcode
                    instructions_for_processing = queue_2[mask_linkcode].copy()
                    # Remove the selected instructions from queue_2
                    queue_2 = queue_2[~mask_linkcode]
                    # Update the starttime for processing
                    instructions_for_processing["Starttime"] = time
                    # Add these instructions back to the queue for reprocessing
                    start_checking_balance = pd.concat([start_checking_balance, instructions_for_processing], ignore_index=True)
    
    cumulative_inserted = pd.concat([cumulative_inserted,start_checking_balance], ignore_index=True)
    #cumulative_inserted = pd.concat([cumulative_inserted,queue_2], ignore_index=True)
    modified_accounts.clear()
    
    return start_checking_balance, queue_2, settled_transactions, event_log, cumulative_inserted

    