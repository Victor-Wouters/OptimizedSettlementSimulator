class Account:
    def __init__(self, account_id, balance, credit_limit):
        self.__account_id = account_id
        self.__balance = balance
        self.__credit_limit = credit_limit

    def get_account_id(self):
        return self.__account_id
    def get_balance(self):
        return self.__balance
    def edit_balance(self, transferred):
        self.__balance = self.__balance + transferred
    def get_credit_limit(self):
        return self.__credit_limit

