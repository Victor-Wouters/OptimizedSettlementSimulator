import pandas as pd
import Eventlog
import datetime


def matching(time, queue_1, start_matching, end_validating, event_log):  
    
    if not end_validating.empty:
        queue_1, start_matching, event_log = matching_insertions(end_validating, queue_1, start_matching, event_log, time) 

    end_validating = pd.DataFrame(columns=end_validating.columns)

    return queue_1, start_matching, end_validating, event_log 

def matching_insertions(insert_transactions, queue_1, start_matching, event_log, time): 
    if queue_1.empty:
        insert_transactions['Starttime'] = pd.NaT
        queue_1 = pd.concat([queue_1, insert_transactions], ignore_index=True)
        for tid in insert_transactions['TID']:
            event_log = Eventlog.Add_to_eventlog(event_log, time, time, tid, activity='Waiting in backlog unmatched')
        return queue_1, start_matching, event_log  

    unmatched_transactions = pd.DataFrame(columns=queue_1.columns)

    for _, row_entry in insert_transactions.iterrows():
        match_indices = queue_1[queue_1['Linkcode'] == row_entry['Linkcode']].index

        if not match_indices.empty:
            
            queue_1.loc[match_indices, 'Starttime'] = time
            matched_transactions = queue_1.loc[match_indices]

            row_entry_df = pd.DataFrame([row_entry], index=match_indices[:1])  # Create a DataFrame for the row_entry
            row_entry_df['Starttime'] = time

            # Concatenate matched transactions to start_matching
            start_matching = pd.concat([start_matching, matched_transactions, row_entry_df], ignore_index=True)

            # Remove matched transaction from queue_1
            queue_1 = queue_1.drop(match_indices)
        else:
            # If no match found, queue the transaction for addition to queue_1
            unmatched_transactions = pd.concat([unmatched_transactions, pd.DataFrame([row_entry])], ignore_index=True)

    # Add unmatched transactions to queue_1 and log them
    if not unmatched_transactions.empty:
        queue_1 = pd.concat([queue_1, unmatched_transactions], ignore_index=True)
        for tid in unmatched_transactions['TID']:
            event_log = Eventlog.Add_to_eventlog(event_log, time, time, tid, activity='Waiting in backlog unmatched')

    return queue_1, start_matching, event_log  


def matching_duration(start_matching, end_matching, current_time, event_log):
    if not start_matching.empty:
        # Calculate the condition directly on the DataFrame
        matching_condition = current_time == (start_matching["Starttime"] + datetime.timedelta(seconds=1))

        # Filter rows that match the condition
        instruction_ended_matching = start_matching[matching_condition].copy()
        
        if not instruction_ended_matching.empty:
            for _, row in instruction_ended_matching.iterrows():
                event_log = Eventlog.Add_to_eventlog(event_log, row["Starttime"], current_time, row['TID'], activity='Matching')

            # Concatenate matching rows to end_matching DataFrame
            end_matching = pd.concat([end_matching, instruction_ended_matching], ignore_index=True)

            # Drop the matching rows from start_matching
            start_matching = start_matching.drop(instruction_ended_matching.index)

    return end_matching, start_matching, event_log
