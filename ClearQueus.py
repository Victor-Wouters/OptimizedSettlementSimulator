import pandas as pd
import Eventlog
import datetime

def clear_queue_to_queue_end_matching(queue_received, queue_cleared, time, event_log):
    # Concatenate queue_received and queue_cleared in a more efficient manner
    queue_received = pd.concat([queue_received, queue_cleared], ignore_index=True)
    
    #if not queue_cleared.empty:
    #    event_log = Eventlog.Add_to_eventlog(event_log, time, time, queue_cleared['TID'], activity='Waiting in queue received')
    
    # Reset queue_cleared efficiently
    queue_cleared = queue_cleared.iloc[0:0]  # This retains the structure but drops all data
    
    return queue_received, queue_cleared, event_log

def send_to_get_cleared(time, event_log, end_matching, start_checking_balance):
    end_matching, start_checking_balance, event_log  = clear_queue_to_queue_end_matching(end_matching, start_checking_balance, time, event_log)

    return event_log, end_matching, start_checking_balance