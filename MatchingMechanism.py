import pandas as pd
import Eventlog
import datetime


def matching(time, opening_time, closing_time, queue_received, queue_1, start_matching, insert_transactions, event_log):
    time_hour = time.time()

    # Optimize adding transactions to queue_received by avoiding row-wise operations
    def add_transactions_to_queue(queue, transactions, event_log, activity):
        # Ensure transactions is not empty and 'TID' column exists
        if not transactions.empty:
            # Concatenate transactions in bulk
            queue = pd.concat([queue, transactions], ignore_index=True)
            # Iterate over 'TID' values and log each transaction
            for tid in transactions['TID']:
                event_log = Eventlog.Add_to_eventlog(event_log, time, time, tid, activity=activity)
        return queue, event_log

    if time_hour < opening_time:
        queue_received, event_log = add_transactions_to_queue(queue_received, insert_transactions, event_log, 'Waiting in queue received')
    elif time_hour == opening_time:
        queue_received, start_matching, event_log = matching_in_queue(queue_received, start_matching, event_log, time)
        queue_received, queue_1, event_log = clear_queue_received(queue_received, queue_1, time, event_log)
    elif opening_time < time_hour < closing_time:
        queue_received, queue_1, start_matching, event_log = matching_insertions(insert_transactions, queue_received, queue_1, start_matching, event_log, time)
    elif time_hour >= closing_time:
        queue_received, event_log = add_transactions_to_queue(queue_received, insert_transactions, event_log, 'Waiting in queue received')

    # Clear insert_transactions efficiently
    insert_transactions = pd.DataFrame(columns=insert_transactions.columns)

    return queue_received, queue_1, start_matching, insert_transactions, event_log

def matching_insertions(insert_transactions, queue_received, queue_1, start_matching, event_log, time):
    if queue_1.empty:
        insert_transactions['Starttime'] = pd.NaT
        queue_1 = pd.concat([queue_1, insert_transactions], ignore_index=True)
        for tid in insert_transactions['TID']:
            event_log = Eventlog.Add_to_eventlog(event_log, time, time, tid, activity='Waiting in queue unmatched')
        return queue_received, queue_1, start_matching, event_log

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
            event_log = Eventlog.Add_to_eventlog(event_log, time, time, tid, activity='Waiting in queue unmatched')

    return queue_received, queue_1, start_matching, event_log

def matching_in_queue(queue_1, start_matching, event_log, time):
    if not queue_1.empty:
        # Calculate value counts directly and filter for Linkcodes occurring exactly twice
        Linkcode_counts = queue_1['Linkcode'].value_counts()
        matching_Linkcodes = Linkcode_counts[Linkcode_counts == 2].index
        
        # Use boolean indexing to find matching rows in a more efficient manner
        is_matching = queue_1['Linkcode'].isin(matching_Linkcodes)
        matched_rows = queue_1[is_matching].copy()
        matched_rows['Starttime'] = time  # Update Starttime for matched rows
        
        # Append matched_rows to start_matching in a single operation
        start_matching = pd.concat([start_matching, matched_rows], ignore_index=True)
        
        # Filter out matched rows from queue_1 using the negation of the is_matching mask
        queue_1 = queue_1[~is_matching]

    return queue_1, start_matching, event_log

def matching_duration(start_matching, end_matching, current_time, event_log):
    if not start_matching.empty:
        # Calculate the condition directly on the DataFrame
        matching_condition = current_time == (start_matching["Starttime"] + datetime.timedelta(seconds=2))

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

def clear_queue_unmatched(queue_received, queue_1, time, event_log):
    # Concatenate queue_received and queue_1 in a more efficient manner
    queue_received = pd.concat([queue_received, queue_1], ignore_index=True)
    
    if not queue_1.empty:
        event_log = Eventlog.Add_to_eventlog(event_log, time, time, queue_1['TID'], activity='Waiting in queue received')
    
    # Reset queue_1 efficiently
    queue_1 = queue_1.iloc[0:0]  # This retains the structure but drops all data
    
    return queue_received, queue_1, event_log

def clear_queue_received(queue_received, queue_1, time, event_log):
    # Concatenate queue_1 and queue_received in a more efficient manner
    queue_1 = pd.concat([queue_1, queue_received], ignore_index=True)
    
    if not queue_received.empty:
        event_log = Eventlog.Add_to_eventlog(event_log, time, time, queue_received['TID'], activity='Waiting in queue unmatched')
    
    # Efficiently reset queue_received to an empty DataFrame with the same structure
    queue_received = queue_received.iloc[0:0]  # Clears all rows but retains the structure
    
    return queue_received, queue_1, event_log