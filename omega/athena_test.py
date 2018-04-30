#!/usr/bin/env python3
import boto3
from time import sleep
import pandas as pd
import os
import glob

# Function for starting athena query
# note Best practice create queries in directory then loop through directory

database = 'litcryptodata'
s3_output = 's3://jckaillitcryptoomega/'
catalog = 'jckaillitcryptoomega'


def run_query(query, database, s3_output):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        }
    )
    print('Execution ID: ' + response['QueryExecutionId'])
    return response


def get_s3_file(qei, catalog):
    import boto3
    file = qei + '.csv'
    s3 = boto3.resource('s3')
    x = cwd + file
    try:
        s3.meta.client.download_file(catalog, file, x)
    except:
        raise
        pass
    return x


def get_status(qei):
    client = boto3.client('athena')
    response = client.get_query_execution(
        QueryExecutionId=qei
    )
    return response['QueryExecution']['Status']['State']


loops = 0
cwd = os.getcwd()

path = cwd + '/athenaqueires/'
extension = 'sql'
os.chdir(path)
query_list = [i for i in glob.glob('*.{}'.format(extension))]
print(query_list)
querydirectory = cwd + '/athenaqueires/'
cwd_split = cwd.split('/')
target_ibdex = cwd_split.index('omega')  # project name
cwd = '/'.join(cwd_split[:target_ibdex - 1]) + '/lit_crypto_data/alpha/data/omega/'

for filename in query_list:
    print('-----------------')
    print('-----------------')
    loops += 1
    x = open(querydirectory + filename)
    full_sql = x.read()
    query = full_sql.replace('\n', ' ')
    print('Running: ' + str(query))
    a = run_query(query, database, s3_output)
    qei = a['QueryExecutionId']
    go = 0
    while go == 0:
        out = get_status(qei)
        if out == 'RUNNING':
            go = 0
        else:
            go = 1

    print(out)
    x = get_s3_file(qei, catalog)

    if loops == 1:
        df = pd.read_csv(x)
        print(df)
    elif loops == 2:
        df2 = pd.read_csv(x)
        print(df2)
    elif loops == 3:
        df3 = pd.read_csv(x)
        print(df3)
    elif loops == 3:
        df4 = pd.read_csv(x)
        print(df4)

# Multiple Linear Regression

# Importing the libraries
import numpy as np
import matplotlib.pyplot as plt

import pandas as pd

# Importing the dataset
# dataset = df
# X = dataset.iloc[:, :-1].values
# y = dataset.iloc[:, 4].values
# #
# # Encoding categorical data
# from sklearn.preprocessing import LabelEncoder, OneHotEncoder
#
# labelencoder = LabelEncoder()
# X[:, 3] = labelencoder.fit_transform(X[:, 3])
#
# onehotencoder = OneHotEncoder(categorical_features=[3])
# X = onehotencoder.fit_transform(X).toarray()
#
# # Avoiding the Dummy Variable Trap
# X = X[:, 1:]
#
# # Splitting the dataset into the Training set and Test set
# from sklearn.cross_validation import train_test_split
#
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
#
# # Fitting Multiple Linear Regression to the Training set
# from sklearn.linear_model import LinearRegression
#
# regressor = LinearRegression()
# regressor.fit(X_train, y_train)
#
# # Predicting the Test set results
# y_pred = regressor.predict(X_test)
#
# plt.scatter(X_train, y_train, color = 'red')
# plt.plot(X_train,regressor.predict(X_train), color = 'blue')
# plt.title('Salary vs Experience(Training set)')
# plt.xlabel('Years of Experience')
# plt.ylabel('Salary')
# plt.show()
