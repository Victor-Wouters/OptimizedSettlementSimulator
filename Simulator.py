import PartAccData
import TransData
import MatchingMechanism
import SettlementMechanism
import Validation
import StatisticsOutput
import LogPartData
import SaveQueues
import ClearQueus
import FreezePart
#import Generator
import pandas as pd
import datetime
import warnings
#import time

def simulator(opening_time, closing_time, recycling, credit_limit_percentage, freeze, freeze_part, freeze_time):

    #read in participant and account data:
    participants = PartAccData.read_csv_and_create_participants('InputData\PARTICIPANTS1.csv', credit_limit_percentage) #Dictionary (key:PartID, value:Part Object)

    #read in transaction data:
    transactions_entry = TransData.read_TRANS('InputData\TRANSACTION1.csv') #Dataframe of all transactions

    # Keep in mind to have the correct format when reading participants in!
    balances_history = pd.DataFrame(columns=['PartID', "Account ID"])
    for i, value in participants.items():
        for j in transactions_entry['FromAccountId'].unique():
            new_row = pd.DataFrame([[value.get_part_id(), value.get_account(j).get_account_id()]],columns=['PartID', 'Account ID'])
            balances_history = pd.concat([balances_history, new_row], ignore_index=True)
    
    final_settlement_efficiency = pd.DataFrame(columns=['Settlement efficiency'])
    SE_over_time = pd.DataFrame()
    cumulative_inserted = pd.DataFrame()
    total_unsettled_value_over_time = pd.DataFrame()
    total_value_waiting_selection_over_time  = pd.DataFrame()
    total_value_waiting_queue_1_over_time = pd.DataFrame()
    total_value_settled_over_time = pd.DataFrame()

    queue_1 = pd.DataFrame()    # Transations waiting to be matched

    start_validating = pd.DataFrame()
    end_validating = pd.DataFrame()

    start_matching = pd.DataFrame()   # Transactions matched, not yet settled
    end_matching = pd.DataFrame(columns=['SettlementDeadline'])

    start_checking_balance = pd.DataFrame()
    end_checking_balance = pd.DataFrame()

    queue_2  = pd.DataFrame()   # Matched, but unsettled
    settled_transactions = pd.DataFrame()   # Transactions settled and completed
    event_log = pd.DataFrame(columns=['TID', 'Starttime', 'Endtime', 'Activity'])   # Event log with all activities


    earliest_datetime = transactions_entry['Time'].min()
    earliest_datetime = earliest_datetime.date()
    start = earliest_datetime

    latest_datetime = transactions_entry['Time'].max()
    latest_datetime = latest_datetime + datetime.timedelta(days=1)
    latest_datetime = latest_datetime.date()
    end = latest_datetime

    midnight = datetime.time(0,0,0)
    start = datetime.datetime.combine(start, midnight)
    end = datetime.datetime.combine(end, midnight)
    total_seconds = int((end - start).total_seconds())


    print("opening time: ")
    print(opening_time)
    print("closing time: ")
    print(closing_time)

    modified_accounts = dict() # RECYCLING AT TIMEPOINTS

    day_counter = 1
    substract_for_next_day = pd.DataFrame()
    settled_transactions_current_day = pd.DataFrame()

    #for i in range(12000): #for debugging
    for i in range(total_seconds):   # For-loop through every second of real-time processing of the business day 86400

        if i % 8640 == 0:
            percent_complete = round((i/(total_seconds-86400))*100)
            bar = '█' * percent_complete + '-' * (100 - percent_complete)
            print(f'\r|{bar}| {percent_complete}% ', end='')

        time = start + datetime.timedelta(seconds=i)
        time_hour = time.time()
        time_day = datetime.datetime(year=time.year, month=time.month, day=time.day)
        time_day = time_day.strftime('%Y-%m-%d')
        current_day = pd.to_datetime(time_day).date()
        
        insert_transactions = transactions_entry[transactions_entry['Time']==time]     # Take all the transactions inserted on this second

        if freeze and time_hour >= freeze_time:

            insert_transactions, queue_1, start_validating, end_validating, start_matching, end_matching, start_checking_balance, end_checking_balance, start_again_checking_balance, end_again_checking_balance, queue_2 = FreezePart.freeze_participant(time_hour, freeze_part, freeze_time, insert_transactions, queue_1, start_validating, end_validating, start_matching, end_matching, start_checking_balance, end_checking_balance, start_again_checking_balance, end_again_checking_balance, queue_2)
        
        end_validating, start_validating, event_log = Validation.validating_duration(insert_transactions, start_validating, end_validating, time, event_log)

        queue_1, start_matching, end_validating, event_log  = MatchingMechanism.matching(time, queue_1, start_matching, end_validating, event_log) # Match inserted transactions
        
        end_matching, start_matching, event_log = MatchingMechanism.matching_duration(start_matching, end_matching, time, event_log)
        
        
        if time_hour >= datetime.time(0,0,1) and time_hour < datetime.time(0,1,0): # Guarantee closed
            
            end_matching['SettlementDeadline'] = pd.to_datetime(end_matching['SettlementDeadline'])
            end_matching_selected = end_matching[end_matching['SettlementDeadline'].dt.date.le(current_day)]
            end_matching = end_matching.drop(end_matching_selected.index)

            cumulative_inserted = pd.concat([cumulative_inserted,end_matching_selected], ignore_index=True)

            end_matching_selected, start_checking_balance, end_checking_balance, queue_2,  settled_transactions, event_log = SettlementMechanism.settle(time, end_matching_selected, start_checking_balance, end_checking_balance, queue_2, settled_transactions, participants, event_log, modified_accounts) # Settle matched transactions

            if time_hour == datetime.time(0,1,0):
                event_log, end_matching, start_checking_balance = ClearQueus.send_to_get_cleared(time, event_log, end_matching, start_checking_balance)

        if time_hour >= opening_time and time_hour < closing_time: # Guarantee closed
            
            end_matching_selected = end_matching[end_matching['SettlementDeadline'].dt.date <= current_day]
            end_matching = end_matching.drop(end_matching_selected.index)

            cumulative_inserted = pd.concat([cumulative_inserted,end_matching_selected], ignore_index=True)

            end_matching_selected, start_checking_balance, end_checking_balance, queue_2,  settled_transactions, event_log = SettlementMechanism.settle(time, end_matching_selected, start_checking_balance, end_checking_balance, queue_2, settled_transactions, participants, event_log, modified_accounts) # Settle matched transactions
        
            if recycling and time_hour == datetime.time(19,20,0):
                
                start_checking_balance, queue_2,  settled_transactions, event_log, cumulative_inserted = SettlementMechanism.atomic_retry_settle(time, start_checking_balance, queue_2, settled_transactions, event_log, modified_accounts, cumulative_inserted)
    
        
        if time_hour == closing_time:       # Empty all activities at close and put in end_matching

            event_log, end_matching, start_checking_balance = ClearQueus.send_to_get_cleared(time, event_log, end_matching, start_checking_balance)

        if i >= 2 * 86400 and i < total_seconds - 86400:

            #if i == 2 * 86400:
             #   cumulative_inserted = pd.DataFrame()

            if i % 900 == 0:
                balances_status = LogPartData.get_partacc_data(participants, transactions_entry)
                time_hour_str = time.strftime('%Y-%m-%d %H:%M:%S')
                
                new_balance_col = pd.DataFrame({time_hour_str: balances_status['Account Balance']})
                balances_history = pd.concat([balances_history, new_balance_col], axis=1)

                SE_timepoint = StatisticsOutput.calculate_SE_over_time(settled_transactions, cumulative_inserted)
                new_SE_col = pd.DataFrame({time_hour_str: SE_timepoint['Settlement efficiency']})
                SE_over_time = pd.concat([SE_over_time, new_SE_col], axis=1)
                
                total_unsettled_value_timepoint = StatisticsOutput.calculate_total_value_unsettled(queue_2)
                new_total_unsettled_value_col = pd.DataFrame({time_hour_str: total_unsettled_value_timepoint['Total value unsettled']})
                total_unsettled_value_over_time = pd.concat([total_unsettled_value_over_time, new_total_unsettled_value_col], axis=1)

                total_value_waiting_selection_timepoint = StatisticsOutput.calculate_total_value_waiting_selection(end_matching)
                new_total_value_waiting_selection_col = pd.DataFrame({time_hour_str: total_value_waiting_selection_timepoint['Total value waiting']})
                total_value_waiting_selection_over_time = pd.concat([total_value_waiting_selection_over_time, new_total_value_waiting_selection_col], axis=1)

                total_value_waiting_queue_1_timepoint = StatisticsOutput.calculate_total_value_waiting_unmatched(queue_1)
                new_total_value_waiting_queue_1_col = pd.DataFrame({time_hour_str: total_value_waiting_queue_1_timepoint['Total value waiting']})
                total_value_waiting_queue_1_over_time = pd.concat([total_value_waiting_queue_1_over_time, new_total_value_waiting_queue_1_col], axis=1)

                total_value_settled_timepoint = StatisticsOutput.calculate_total_value_settled(settled_transactions)
                new_total_value_settled_col = pd.DataFrame({time_hour_str: total_value_settled_timepoint['Total value settled']})
                total_value_settled_over_time = pd.concat([total_value_settled_over_time, new_total_value_settled_col], axis=1)

            if i == (total_seconds-86401):
                balances_status = LogPartData.get_partacc_data(participants, transactions_entry)
                time_hour_str = time.strftime('%Y-%m-%d %H:%M:%S')
                
                new_balance_col = pd.DataFrame({time_hour_str: balances_status['Account Balance']})
                balances_history = pd.concat([balances_history, new_balance_col], axis=1)

                SE_timepoint = StatisticsOutput.calculate_SE_over_time(settled_transactions, cumulative_inserted)
                new_SE_col = pd.DataFrame({time_hour_str: SE_timepoint['Settlement efficiency']})
                SE_over_time = pd.concat([SE_over_time, new_SE_col], axis=1)
                
                total_unsettled_value_timepoint = StatisticsOutput.calculate_total_value_unsettled(queue_2)
                new_total_unsettled_value_col = pd.DataFrame({time_hour_str: total_unsettled_value_timepoint['Total value unsettled']})
                total_unsettled_value_over_time = pd.concat([total_unsettled_value_over_time, new_total_unsettled_value_col], axis=1)

                total_value_waiting_selection_timepoint = StatisticsOutput.calculate_total_value_waiting_selection(end_matching)
                new_total_value_waiting_selection_col = pd.DataFrame({time_hour_str: total_value_waiting_selection_timepoint['Total value waiting']})
                total_value_waiting_selection_over_time = pd.concat([total_value_waiting_selection_over_time, new_total_value_waiting_selection_col], axis=1)

                total_value_waiting_queue_1_timepoint = StatisticsOutput.calculate_total_value_waiting_unmatched(queue_1)
                new_total_value_waiting_queue_1_col = pd.DataFrame({time_hour_str: total_value_waiting_queue_1_timepoint['Total value waiting']})
                total_value_waiting_queue_1_over_time = pd.concat([total_value_waiting_queue_1_over_time, new_total_value_waiting_queue_1_col], axis=1)

                total_value_settled_timepoint = StatisticsOutput.calculate_total_value_settled(settled_transactions)
                new_total_value_settled_col = pd.DataFrame({time_hour_str: total_value_settled_timepoint['Total value settled']})
                total_value_settled_over_time = pd.concat([total_value_settled_over_time, new_total_value_settled_col], axis=1)

            if (i % 86400 == 0 and i!=2 * 86400) or (i == (total_seconds-86401)):
                if day_counter == 1:
                    substract_for_next_day = settled_transactions
                    settled_transactions_current_day = settled_transactions
                    SaveQueues.save_queues(queue_1, end_matching, settled_transactions_current_day, queue_2, day_counter)
                    final_settlement_efficiency = StatisticsOutput.calculate_total_SE(cumulative_inserted, settled_transactions_current_day, final_settlement_efficiency)
                    StatisticsOutput.calculate_SE_per_participant(cumulative_inserted, settled_transactions_current_day, day_counter)
                    StatisticsOutput.statistics_generate_output_SE(final_settlement_efficiency, day_counter)
                    cumulative_inserted = pd.DataFrame()
                    day_counter = day_counter + 1
                else:
                    settled_transactions_current_day = pd.concat([substract_for_next_day, settled_transactions]).drop_duplicates(keep=False)
                    substract_for_next_day = settled_transactions
                    SaveQueues.save_queues(queue_1, end_matching, settled_transactions_current_day, queue_2, day_counter)
                    final_settlement_efficiency = StatisticsOutput.calculate_total_SE(cumulative_inserted, settled_transactions_current_day, final_settlement_efficiency)
                    StatisticsOutput.calculate_SE_per_participant(cumulative_inserted, settled_transactions_current_day, day_counter)
                    StatisticsOutput.statistics_generate_output_SE(final_settlement_efficiency, day_counter)
                    cumulative_inserted = pd.DataFrame()
                    day_counter = day_counter + 1
        if i == total_seconds - 86400:
            break

    event_log.to_csv('eventlog\\eventlog.csv', index=False, sep = ';')

    max_credit = LogPartData.balances_history_calculations(balances_history, participants)

    StatisticsOutput.statistics_generate_output(total_unsettled_value_over_time, SE_over_time, total_value_waiting_selection_over_time, total_value_waiting_queue_1_over_time, total_value_settled_over_time)

    max_unsettled_value = total_unsettled_value_over_time.iloc[0].max()

    return max_credit, final_settlement_efficiency, max_unsettled_value

if __name__ == '__main__':

    warnings.simplefilter(action='ignore', category=FutureWarning)

    #Initializations:
    opening_time = datetime.time(1,30,0)
    closing_time = datetime.time(19,30,00) #19u30 base
    recycling = True
    credit_limit_percentage = 1.0

    # Freeze participant
    freeze = False
    freeze_part = '41'
    freeze_time = datetime.time(14,00,00)

    start_time = datetime.datetime.now()
    print("Start Time:", start_time.strftime('%Y-%m-%d %H:%M:%S'))

    max_credit, final_settlement_efficiency, max_unsettled_value = simulator(opening_time, closing_time, recycling, credit_limit_percentage, freeze, freeze_part, freeze_time)

    end_time = datetime.datetime.now()
    print("End Time:", end_time.strftime('%Y-%m-%d %H:%M:%S'))
    duration = end_time - start_time
    print("Execution Duration:", duration)