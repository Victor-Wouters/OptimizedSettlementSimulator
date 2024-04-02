import pandas as pd

def save_queues(queue_1,end_matching,settled_transactions,queue_2):
    queue_1.to_csv('EndQueues\\queue_1.csv', index=False, sep = ';')
    end_matching.to_csv('EndQueues\\end_matching.csv', index=False, sep = ';')
    settled_transactions.to_csv('EndQueues\\settled_transactions.csv', index=False, sep = ';')
    queue_2.to_csv('EndQueues\\queue_2.csv', index=False, sep = ';')
