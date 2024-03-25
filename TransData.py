import pandas as pd

def read_TRANS(filename):

    dtype_spec = {'TID': str,'FromParticipantId': str, 'FromAccountId': str, 'ToParticipantId': str, 'ToAccountId': str}

    DF_TRANS = pd.read_csv(filename,sep=';', dtype=dtype_spec, parse_dates=['Time'])

    return DF_TRANS

