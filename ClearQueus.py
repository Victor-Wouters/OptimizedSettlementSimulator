import pandas as pd
import Eventlog
import datetime

def clear_queue_to_queue_received(queue_received, queue_cleared, time, event_log):
    queue_received = pd.concat([queue_received,queue_cleared], ignore_index=True)
    for _, row_queue_cleared in queue_cleared.iterrows():
        event_log = Eventlog.Add_to_eventlog(event_log, time, time, row_queue_cleared['TID'], activity='Waiting in queue received')
    queue_cleared = pd.DataFrame(columns=queue_cleared.columns)

    return queue_received, queue_cleared, event_log