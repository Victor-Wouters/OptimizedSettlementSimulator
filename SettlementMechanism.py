import pandas as pd
import Eventlog
import datetime

def settle(time, end_matching, start_checking_balance, end_checking_balance, start_settlement_execution, end_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts):
    # Add 'Starttime' at the beginning to ensure it exists
    for df in [start_checking_balance, start_settlement_execution]:
        if 'Starttime' not in df.columns:
            df['Starttime'] = pd.NaT

    # Process matched instructions
    if not end_matching.empty:
        end_matching['Starttime'] = time
        start_checking_balance = pd.concat([start_checking_balance, end_matching], ignore_index=True)
        end_matching = end_matching.iloc[0:0]  # Clear efficiently

    # Checking balance and credit activity
    check_time_limit = time - datetime.timedelta(seconds=2)
    mask = start_checking_balance['Starttime'] == check_time_limit
    instructions_ready_for_check = start_checking_balance[mask]
    if not instructions_ready_for_check.empty:
        event_log = Eventlog.Add_to_eventlog(event_log, instructions_ready_for_check['Starttime'], time, instructions_ready_for_check['TID'], activity='Checking balance and credit')
        end_checking_balance = pd.concat([end_checking_balance, instructions_ready_for_check], ignore_index=True)
    start_checking_balance = start_checking_balance[~mask]

    # Process checking balance and settlement
    if not end_checking_balance.empty:
        for linkcode in end_checking_balance['Linkcode'].unique():
            instructions_for_processing = end_checking_balance[end_checking_balance['Linkcode'] == linkcode].copy()
            settlement_confirmation = check_balance(True, instructions_for_processing, participants)
            
            if settlement_confirmation:
                instructions_for_processing['Starttime'] = time
                start_settlement_execution = pd.concat([start_settlement_execution, instructions_for_processing], ignore_index=True)
            else:
                queue_2 = pd.concat([queue_2, instructions_for_processing], ignore_index=True)
                event_log = Eventlog.Add_to_eventlog(event_log, time, time, instructions_for_processing['TID'], activity='Waiting in queue unsettled')
        end_checking_balance = pd.DataFrame()

    # Settlement execution
    exec_time_limit = time - datetime.timedelta(seconds=2)
    mask = start_settlement_execution['Starttime'] == exec_time_limit
    instructions_ready_for_exec = start_settlement_execution[mask]
    if not instructions_ready_for_exec.empty:
        event_log = Eventlog.Add_to_eventlog(event_log, instructions_ready_for_exec['Starttime'], time, instructions_ready_for_exec['TID'], activity='Settling')
        end_settlement_execution = pd.concat([end_settlement_execution, instructions_ready_for_exec], ignore_index=True)
    start_settlement_execution = start_settlement_execution[~mask]

    # Final settlement processing
    if not end_settlement_execution.empty:
        for linkcode in end_settlement_execution['Linkcode'].unique():
            instructions_for_processing = end_settlement_execution[end_settlement_execution['Linkcode'] == linkcode].copy()
            settlement_execution(instructions_for_processing, participants)
            modified_accounts = keep_track_modified_accounts(instructions_for_processing, modified_accounts)
            settled_transactions = pd.concat([settled_transactions, instructions_for_processing], ignore_index=True)
        end_settlement_execution = pd.DataFrame()

    return end_matching, start_checking_balance, end_checking_balance, start_settlement_execution, end_settlement_execution, queue_2, settled_transactions, event_log

def check_balance(settlement_confirmation, instructions_for_processing, participants):

    for i in [0,1]:
                from_part = instructions_for_processing['FromParticipantId'].iloc[i]
                from_acc = instructions_for_processing['FromAccountId'].iloc[i]
                transaction_value = instructions_for_processing['Value'].iloc[i]
                current_balance = participants.get(from_part).get_account(from_acc).get_balance()
                credit_limit = participants.get(from_part).get_account(from_acc).get_credit_limit()
                if transaction_value > (current_balance+credit_limit):
                    return False
                    
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

def retry_settle(time, start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts): 
     
    if not queue_2.empty:
        for key, value in modified_accounts.items():
            first_instruction = queue_2[(queue_2["FromParticipantId"] == key) & (queue_2["FromAccountId"] == value)] 
            retry_linkcodes = first_instruction['Linkcode'].unique()
            for linkcode in retry_linkcodes:
                instructions_for_processing = queue_2[queue_2["Linkcode"] == linkcode].copy()
                queue_2 = queue_2[queue_2['Linkcode'] != linkcode]
                
                instructions_for_processing["Starttime"] = time
                # Save for 2 sec before checking
                start_again_checking_balance = pd.concat([start_again_checking_balance,instructions_for_processing], ignore_index=True)
                
    rows_to_remove = []
    for index, instruction_checking in start_again_checking_balance.iterrows():
        if time == (instruction_checking["Starttime"] + datetime.timedelta(seconds=2)):
            instruction_ended_checking = pd.DataFrame([instruction_checking])
            event_log = Eventlog.Add_to_eventlog(event_log, instruction_checking["Starttime"], time, instruction_ended_checking['TID'], activity='Checking balance and credit')
            end_again_checking_balance = pd.concat([end_again_checking_balance,instruction_ended_checking], ignore_index=True)
            rows_to_remove.append(index)
    start_again_checking_balance = start_again_checking_balance.drop(rows_to_remove)
    
    if not end_again_checking_balance.empty:
        retry_linkcodes = end_again_checking_balance['Linkcode'].unique()
        for linkcode in retry_linkcodes:
            instructions_for_processing = end_again_checking_balance[end_again_checking_balance["Linkcode"] == linkcode].copy()
            end_again_checking_balance = end_again_checking_balance[end_again_checking_balance['Linkcode'] != linkcode]

            settlement_confirmation = True
            
            # check balance and if not ok: reject settlement
            settlement_confirmation = check_balance(settlement_confirmation, instructions_for_processing, participants)

                # if settlement confirmed, edit balances, transactions: move from matched transactions to settled transactions
            if settlement_confirmation == True:
                instructions_for_processing["Starttime"] = time
                # Save for 2 sec before settlement execution
                start_again_settlement_execution = pd.concat([start_again_settlement_execution,instructions_for_processing], ignore_index=True)
            else: 
                queue_2 = pd.concat([queue_2,instructions_for_processing], ignore_index=True)   # if settlement rejected, move transactions to queue 2
                for i in [0,1]: # log in eventlog
                    event_log = Eventlog.Add_to_eventlog(event_log, time, time, instructions_for_processing['TID'].iloc[i], activity='Waiting in queue unsettled') #Waiting again in queue unsettled   
                     
    rows_to_remove = []
    for index, instruction_executing in start_again_settlement_execution.iterrows():
        if time == (instruction_executing["Starttime"] + datetime.timedelta(seconds=2)):
            instruction_ended_executing = pd.DataFrame([instruction_executing])
            event_log = Eventlog.Add_to_eventlog(event_log, instruction_executing["Starttime"], time, instruction_ended_executing['TID'], activity='Settling') #Settling from queue unsettled
            end_again_settlement_execution = pd.concat([end_again_settlement_execution,instruction_ended_executing], ignore_index=True)
            rows_to_remove.append(index)
    start_again_settlement_execution = start_again_settlement_execution.drop(rows_to_remove)

    if not end_again_settlement_execution.empty:
        retry_linkcodes = end_again_settlement_execution['Linkcode'].unique()
        for linkcode in retry_linkcodes:
            instructions_for_processing = end_again_settlement_execution[end_again_settlement_execution['Linkcode'] == linkcode].copy()
            end_again_settlement_execution = end_again_settlement_execution[end_again_settlement_execution['Linkcode'] != linkcode]            
               
            settlement_execution(instructions_for_processing, participants)
            settled_transactions = pd.concat([settled_transactions,instructions_for_processing], ignore_index=True)

    return start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2,  settled_transactions, event_log

def atomic_retry_settle(time, start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts):
    # Ensure 'Starttime' column exists in the DataFrame
    if 'Starttime' not in start_again_checking_balance.columns:
        start_again_checking_balance['Starttime'] = pd.NaT

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
                    start_again_checking_balance = pd.concat([start_again_checking_balance, instructions_for_processing], ignore_index=True)

    # Check for transactions ready for balance and credit checking
    time_limit = time - datetime.timedelta(seconds=2)
    mask = start_again_checking_balance["Starttime"] == time_limit
    instructions_ready_for_check = start_again_checking_balance[mask]
    if not instructions_ready_for_check.empty:
        event_log = Eventlog.Add_to_eventlog(event_log, instructions_ready_for_check["Starttime"], time, instructions_ready_for_check['TID'], activity='Checking balance and credit')
        end_again_checking_balance = pd.concat([end_again_checking_balance, instructions_ready_for_check], ignore_index=True)
    start_again_checking_balance = start_again_checking_balance[~mask]

    # Process transactions after balance check
    if not end_again_checking_balance.empty:
        for linkcode in end_again_checking_balance['Linkcode'].unique():
            mask_linkcode = end_again_checking_balance["Linkcode"] == linkcode
            instructions_for_processing = end_again_checking_balance[mask_linkcode].copy()
            end_again_checking_balance = end_again_checking_balance[~mask_linkcode]

            settlement_confirmation = check_balance(True, instructions_for_processing, participants)

            if settlement_confirmation:
                instructions_for_processing["Starttime"] = time
                settlement_execution(instructions_for_processing, participants)
                event_log = Eventlog.Add_to_eventlog(event_log, instructions_for_processing["Starttime"], time, instructions_for_processing['TID'], activity='Settling')
                settled_transactions = pd.concat([settled_transactions, instructions_for_processing], ignore_index=True)
            else:
                queue_2 = pd.concat([queue_2, instructions_for_processing], ignore_index=True)
                event_log = Eventlog.Add_to_eventlog(event_log, time, time, instructions_for_processing['TID'], activity='Waiting in queue unsettled')

    return start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2, settled_transactions, event_log

    