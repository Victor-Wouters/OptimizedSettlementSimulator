import pandas as pd



def freeze_participant(time_hour, freeze_part, freeze_time, insert_transactions, queue_1, start_validating, end_validating, start_matching, end_matching, start_checking_balance, end_checking_balance, start_again_checking_balance, end_again_checking_balance, queue_2):
    
    insert_transactions = insert_transactions[(insert_transactions['FromParticipantId'] != freeze_part) & (insert_transactions['ToParticipantId'] != freeze_part)]
    
    if time_hour == freeze_time:
        required_columns = ['FromParticipantId', 'ToParticipantId']

        if all(column in queue_1.columns for column in required_columns):
            queue_1 = queue_1[(queue_1['FromParticipantId'] != freeze_part) & (queue_1['ToParticipantId'] != freeze_part)]

        if all(column in start_validating.columns for column in required_columns):
            start_validating = start_validating[(start_validating['FromParticipantId'] != freeze_part) & (start_validating['ToParticipantId'] != freeze_part)]

        if all(column in end_validating.columns for column in required_columns):
            end_validating = end_validating[(end_validating['FromParticipantId'] != freeze_part) & (end_validating['ToParticipantId'] != freeze_part)]

        if all(column in start_matching.columns for column in required_columns):
            start_matching = start_matching[(start_matching['FromParticipantId'] != freeze_part) & (start_matching['ToParticipantId'] != freeze_part)]

        if all(column in end_matching.columns for column in required_columns):
            end_matching = end_matching[(end_matching['FromParticipantId'] != freeze_part) & (end_matching['ToParticipantId'] != freeze_part)]

        if all(column in start_checking_balance.columns for column in required_columns):
            start_checking_balance = start_checking_balance[(start_checking_balance['FromParticipantId'] != freeze_part) & (start_checking_balance['ToParticipantId'] != freeze_part)]

        if all(column in end_checking_balance.columns for column in required_columns):
            end_checking_balance = end_checking_balance[(end_checking_balance['FromParticipantId'] != freeze_part) & (end_checking_balance['ToParticipantId'] != freeze_part)]

        if all(column in start_again_checking_balance.columns for column in required_columns):
            start_again_checking_balance = start_again_checking_balance[(start_again_checking_balance['FromParticipantId'] != freeze_part) & (start_again_checking_balance['ToParticipantId'] != freeze_part)]

        if all(column in end_again_checking_balance.columns for column in required_columns):
            end_again_checking_balance = end_again_checking_balance[(end_again_checking_balance['FromParticipantId'] != freeze_part) & (end_again_checking_balance['ToParticipantId'] != freeze_part)]

        if all(column in queue_2.columns for column in required_columns):
            queue_2 = queue_2[(queue_2['FromParticipantId'] != freeze_part) & (queue_2['ToParticipantId'] != freeze_part)]

    return  insert_transactions, queue_1, start_validating, end_validating, start_matching, end_matching, start_checking_balance, end_checking_balance, start_again_checking_balance, end_again_checking_balance, queue_2