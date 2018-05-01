# Data Preprocessing Template

# Importing the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os

cwd = os.getcwd()
# print(cwd)

# Importing the dataset
dataset = pd.read_csv('/Users/jkail/Documents/GitHub/lit_crypto_data/alpha/data/omega/0a181325-7388-45ca-aff6-d19e76db6fb1.csv')
# print(dataset)
dataset = dataset.fillna(0)
X = dataset.iloc[:, 1:-1].values
y = dataset.iloc[:, 5].values

# print(X)

# Encoding categorical data
# Encoding the Independent Variable
# from sklearn.preprocessing import LabelEncoder, OneHotEncoder
#
# labelencoder_X = LabelEncoder()
# X[:, 3] = labelencoder_X.fit_transform(X[:, 3])
# onehotencoder = OneHotEncoder(categorical_features=[3])
# X = onehotencoder.fit_transform(X).toarray()
# Encoding the Dependent Variable
# labelencoder_y = LabelEncoder()
# y = labelencoder_y.fit_transform(y)
# print(X)
# print(y)

# Avoiding the Dummy Variable Trap
#X = X[:, 1:]

# Splitting the dataset into the Training set and Test set
from sklearn.cross_validation import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)



# Fitting Multiple Linear Regression to the Training set
from sklearn.linear_model import LinearRegression

regressor = LinearRegression()
regressor.fit(X_train, y_train)
print('------------predict pre backwards elimination----------------------------------------------------------------------------------------------------------------------------------------------------')
print(' test,     prediction')
# Predicting
y_pred = regressor.predict(X_test)
#print(y_test)
df_y_test = pd.DataFrame(y_test)
#print(df_y_test)
df_y_pred = pd.DataFrame(y_pred)
#print(y_pred)

result = pd.concat([df_y_test, df_y_pred], axis=1, join_axes=[df_y_test.index])
print(result)


import statsmodels.formula.api as sm
#print('----------------------------------------------------------------------------------------------------------------------------------------------------------------')
array_len = len(X)
X = np.append(arr=np.ones((array_len, 1)).astype(int), values = X, axis=  1) # addconstant to first row
#print('----------------------------------------------------------------------------------------------------------------------------------------------------------------')
# print(X)
X_opt = X[:, [0,1,2,3,4]]
# print(X_opt)
regressor_OLS = sm.OLS(endog = y, exog = X_opt).fit()
a = regressor_OLS.summary() #creates table summery of dataset
#print(a)


import statsmodels.formula.api as sm
def backwardElimination(x, SL, array_len):
    numVars = len(x[0])
    temp = np.zeros((array_len,6)).astype(int)
    for i in range(0, numVars):
        regressor_OLS = sm.OLS(y, x).fit()
        maxVar = max(regressor_OLS.pvalues).astype(float)
        adjR_before = regressor_OLS.rsquared_adj.astype(float)
        if maxVar > SL:
            for j in range(0, numVars - i):
                if (regressor_OLS.pvalues[j].astype(float) == maxVar):
                    temp[:,j] = x[:, j]
                    x = np.delete(x, j, 1)
                    tmp_regressor = sm.OLS(y, x).fit()
                    adjR_after = tmp_regressor.rsquared_adj.astype(float)
                    if (adjR_before >= adjR_after):
                        x_rollback = np.hstack((x, temp[:,[0,j]]))
                        x_rollback = np.delete(x_rollback, j, 1)
                        print (regressor_OLS.summary())
                        return x_rollback
                    else:
                        continue
    print(regressor_OLS.summary())
    return x

SL = 0.05

X_Modeled = backwardElimination(X_opt, SL, array_len)


X_train, X_test, y_train, y_test = train_test_split(X_Modeled, y, test_size=0.2, random_state=0)



# Fitting Multiple Linear Regression to the Training set
from sklearn.linear_model import LinearRegression

regressor = LinearRegression()
regressor.fit(X_train, y_train)
print('------------predict post backwards elimination----------------------------------------------------------------------------------------------------------------------------------------------------')
print(' test,     prediction')
# Predicting
y_pred = regressor.predict(X_test)
#print(y_test)
df_y_test = pd.DataFrame(y_test)
#print(df_y_test)
df_y_pred = pd.DataFrame(y_pred)
#print(y_pred)

result = pd.concat([df_y_test, df_y_pred], axis=1, join_axes=[df_y_test.index])
print(result)
