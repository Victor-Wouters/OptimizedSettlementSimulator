import pandas as pd

def save_queues(queue_1,end_matching,settled_transactions,queue_2, day_counter):
    queue_1.to_csv(f'EndQueues\\queue_1_day_{day_counter}.csv', index=False, sep = ';')
    end_matching.to_csv(f'EndQueues\\end_matching_day_{day_counter}.csv', index=False, sep = ';')
    settled_transactions.to_csv(f'EndQueues\\settled_transactions_day_{day_counter}.csv', index=False, sep = ';')
    queue_2.to_csv(f'EndQueues\\queue_2_day_{day_counter}.csv', index=False, sep = ';')
