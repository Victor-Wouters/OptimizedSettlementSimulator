import pandas as pd

def Add_to_eventlog(event_log, starttime, endtime, IDs, activity):
    """
    Add multiple records to the event log in bulk.
    
    Parameters:
    - event_log (pd.DataFrame): The existing event log DataFrame.
    - starttime (datetime): The start time for the entries.
    - endtime (datetime): The end time for the entries.
    - IDs (pd.Series or single value): Transaction IDs to add. Can be a single ID or a Series of IDs.
    - activity (str): The activity description for the entries.
    
    Returns:
    - pd.DataFrame: Updated event log with new records added.
    """
    # Check if IDs is a Series (multiple IDs) and create a DataFrame if so
    if isinstance(IDs, pd.Series):
        new_rows = pd.DataFrame({
            'TID': IDs,
            'Starttime': starttime,
            'Endtime': endtime,
            'Activity': activity
        })
    else:  # Handle a single ID
        new_rows = pd.DataFrame({
            'TID': [IDs],
            'Starttime': [starttime],
            'Endtime': [endtime],
            'Activity': [activity]
        })
    
    # Concatenate the new rows to the existing event_log DataFrame
    event_log = pd.concat([event_log, new_rows], ignore_index=True)
    
    return event_log