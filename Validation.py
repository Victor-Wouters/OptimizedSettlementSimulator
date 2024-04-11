import pandas as pd
import Eventlog
import datetime


def validating_duration(insert_transactions, start_validating, end_validating, current_time, event_log):
    # Directly assign current_time to 'Starttime' for all insert_transactions
    if not insert_transactions.empty:
        insert_transactions_copy = insert_transactions.copy()
        insert_transactions_copy["Starttime"] = current_time
        start_validating = pd.concat([start_validating, insert_transactions_copy], ignore_index=True)

    # Identify rows where 'Starttime' + 2 seconds equals current_time
    if not start_validating.empty:
        valid_time = start_validating["Starttime"] + datetime.timedelta(seconds=1)
        valid_rows = current_time == valid_time

        # Filter rows that satisfy the condition
        instruction_ended_validating = start_validating[valid_rows]

        # Update the event log for each valid instruction
        if not instruction_ended_validating.empty:
            for _, row in instruction_ended_validating.iterrows():
                event_log = Eventlog.Add_to_eventlog(event_log, row["Starttime"], current_time, row['TID'], activity='Validating')
            
            # Add these rows to end_validating
            end_validating = pd.concat([end_validating, instruction_ended_validating], ignore_index=True)

            # Remove the processed rows from start_validating
            start_validating = start_validating.drop(instruction_ended_validating.index)

    return end_validating, start_validating, event_log