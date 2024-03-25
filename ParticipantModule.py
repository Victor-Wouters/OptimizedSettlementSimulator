import AccountModule

class Participant:
    def __init__(self, part_id):
        self.__part_id = part_id
        self.__accounts = {}

    def get_part_id(self):
        return self.__part_id

    def add_account(self, account_id, balance, credit_limit):
        # Check if the account ID already exists to prevent duplicates, normally not a problem because of the input
        if account_id in self.__accounts:
            print(f"Account ID {account_id} already exists for participant {self.part_id}.")
        else:
            self.__accounts[account_id] = AccountModule.Account(account_id, balance, credit_limit)
            
    def get_account(self, account_id):
        # Retrieve an account by its ID
        return self.__accounts.get(account_id, None)


