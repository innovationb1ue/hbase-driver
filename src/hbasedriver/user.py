class User:
    def __init__(self, hadoop_user_name: str = 'default', hadoop_proxy_user: str = ''):
        self.hadoop_user_name = hadoop_user_name
        self.hadoop_proxy_user = hadoop_proxy_user
