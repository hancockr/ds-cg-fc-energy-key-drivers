# Databricks notebook source
# MAGIC %run "/Repos/hancock.r@pg.com/ds-cg-fc-energy-key-drivers/Key Drivers functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # import key libraries
# MAGIC import pyspark
# MAGIC from pyspark.sql.types import IntegerType, FloatType, DateType
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC import datetime
# MAGIC from datetime import date
# MAGIC from datetime import datetime as dt
# MAGIC import pytz 
# MAGIC import seaborn as sns
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC from sklearn.ensemble import RandomForestRegressor
# MAGIC from sklearn.inspection import permutation_importance
# MAGIC from sklearn.model_selection import KFold
# MAGIC from scipy import stats
# MAGIC from sklearn.tree import DecisionTreeRegressor
# MAGIC from sklearn import tree
# MAGIC from sklearn.linear_model import LinearRegression
# MAGIC from sklearn.linear_model import Ridge 
# MAGIC from sklearn.linear_model import Lasso
# MAGIC from sklearn import model_selection 
# MAGIC import matplotlib
# MAGIC import matplotlib.pyplot as plt 
# MAGIC import seaborn as sns
# MAGIC from sklearn.mixture import GaussianMixture
# MAGIC from scipy.signal import find_peaks
# MAGIC from sklearn.feature_selection import VarianceThreshold
# MAGIC
# MAGIC import warnings 
# MAGIC warnings.filterwarnings("ignore")
# MAGIC
# MAGIC import os
# MAGIC os.system("")
# MAGIC class style():
# MAGIC     RED = '\033[31m'
# MAGIC     
# MAGIC # site and machine dictionary
# MAGIC # @Glori, please updatet the site_machine_dict when our data from other machine comes in: 
# MAGIC
# MAGIC site_machine_dict = {'ay':['1a','2a','3a','4a','5a','6a'],
# MAGIC                     'beu':['15b'],
# MAGIC                    'cg':['5g','6g','7g'],
# MAGIC                    'gb':['10f','11f','12f','13f','14f','15f'],
# MAGIC                    'mp':['1m','2m','3m','4m','5m','6m','7m','8m'],
# MAGIC                    'ox':['1x','2x']}
# MAGIC
# MAGIC # Convert dictionary to dataframe 
# MAGIC site_machine_df = pd.DataFrame (((i,j)for i in site_machine_dict.keys() for j in site_machine_dict[i]), columns = ['site','machines'])
# MAGIC
# MAGIC # machine leanring model dictionary
# MAGIC model_dict={'LinearRegression':LinearRegression(),'Ridge':Ridge(),'Lasso':Lasso(),'RandomForestRegressor':RandomForestRegressor()}
# MAGIC
# MAGIC def creat_sql(select_site, select_machine):
# MAGIC     """
# MAGIC     Function that create sql that allow users to interact with database
# MAGIC     Parameters:
# MAGIC     select_site: str - Site name user selected
# MAGIC     select_machine: str - Machine name user selected
# MAGIC     """
# MAGIC     # build machine_table 
# MAGIC     machine_table = '_'.join([select_site,select_machine,'timeseries'])
# MAGIC     # build data_table
# MAGIC     data_table = '.'.join(['groupdb_famc_energy_analytics',machine_table])
# MAGIC     # build sql 
# MAGIC     sql = 'select * from ' +data_table
# MAGIC     return sql
# MAGIC
# MAGIC def create_widget(name, data, feature, type):
# MAGIC     """
# MAGIC     Function that creates widges in the notebook that allow users interact with the code
# MAGIC     Parameters:
# MAGIC     name: str - Name desired for the widget
# MAGIC     data: pd.DataFrame - dataframe containing the data needed to create the widget
# MAGIC     feature: str - name of the column to be used to extract the unique values from data
# MAGIC     """
# MAGIC     items = data[feature].unique()
# MAGIC     if type == 'dropdown':
# MAGIC         dbutils.widgets.dropdown(name, items[0], [x for x in items])
# MAGIC     elif type == 'multiselect':
# MAGIC         dbutils.widgets.multiselect(name, items[0], [x for x in items])
# MAGIC     else:
# MAGIC         print('Widget type not recognized')
# MAGIC
# MAGIC def historical_data (data, n_months):
# MAGIC     """
# MAGIC     Helper function to get the historical data
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed to retrieve historical data
# MAGIC     n_months: int - Number of months the user can track from historoical data, defaulted to be the last 18 months
# MAGIC     """
# MAGIC     today = date.today()
# MAGIC     
# MAGIC     #past_date  = today - pd.DateOffset(months=n_months)
# MAGIC     past_date  = today - pd.DateOffset(months=n_months)
# MAGIC     
# MAGIC     data = data[data['time_bucket_local'] >= str(past_date)]
# MAGIC     
# MAGIC     return data
# MAGIC
# MAGIC def prod_codes_sorting(data):
# MAGIC     """
# MAGIC     Helper function that sort the prod_codes of one machine by Descending order via number of datapoints
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed to retrieve prod_codes
# MAGIC     """ 
# MAGIC     prod_code = data.groupby('prod_code')['time_bucket_local'].count().reset_index().sort_values(by='time_bucket_local',ascending = False)
# MAGIC     return prod_code
# MAGIC
# MAGIC def get_time_series_data (data,select_prod_codes):
# MAGIC     """
# MAGIC     Function that create training data based on the prod_codes that have been selected
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed to create training data
# MAGIC     """ 
# MAGIC      # filter to only selected prod_codes
# MAGIC     data = data [data['prod_code'].isin(select_prod_codes)] 
# MAGIC     
# MAGIC     # get rid of redundant columns 
# MAGIC     time_series_data = data.iloc[:,2:]
# MAGIC     time_series_data.drop(['machine','time_bucket_utc'],axis = 1,inplace = True)
# MAGIC     
# MAGIC     return time_series_data   
# MAGIC
# MAGIC def get_prod_data (data,select_prod_codes):
# MAGIC     """
# MAGIC     Function that create training data based on the prod_codes that have been selected
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed to create training data
# MAGIC     select_prod_codes: list - list conatining the select_prod_codes 
# MAGIC     """
# MAGIC     # filter to only selected prod_codes
# MAGIC     data = data [data['prod_code'].isin(select_prod_codes)]  
# MAGIC     
# MAGIC     return data  
# MAGIC
# MAGIC def remove_outlier(df):
# MAGIC     df = df.dropna()
# MAGIC     # define outlier function for each column 
# MAGIC     def outliers(df, ft): 
# MAGIC         
# MAGIC         lower_bound = df[ft].quantile(0.05)
# MAGIC         upper_bound = df[ft].quantile(0.99)
# MAGIC         
# MAGIC         ft_index = df.index[(df[ft]<lower_bound)|(df[ft]>upper_bound)]
# MAGIC         return ft_index
# MAGIC     
# MAGIC     remove_index =[]
# MAGIC     for col in df.columns:
# MAGIC         remove_index.extend(outliers(df, col))
# MAGIC         
# MAGIC     remove_index=sorted(set(remove_index))    
# MAGIC     df = df.drop(remove_index)    
# MAGIC     return df 
# MAGIC
# MAGIC # The following section is for auto_visaulizatin DTreeReg_mix_gaussian_splits testing
# MAGIC def get_gmm_splits(data, variable):     
# MAGIC     """
# MAGIC     Function that splits the data base on the GMM algo. 
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed to get peaks
# MAGIC     variable: str - the variable that for peak detection
# MAGIC     """ 
# MAGIC         
# MAGIC     # extract variable data 
# MAGIC     data = np.array(data[variable])
# MAGIC     # select the best n_numbers, looping from 1 to 3 >> edge case that max(data) == min (data), which means the variable is consant, then the best_n_components is defaluted to be 1 culster only.
# MAGIC     #compare with n_components best fit the data, ranging from 1 to 3.
# MAGIC     # based on the matric of gmm.bic Bayesian information criterion (BIC):This criterion gives us an estimation on how much is good the GMM in terms of predicting the data we actually have. The lower is the BIC, the better is the model to actually predict the data we have, and by extension, the true, unknown, distribution.
# MAGIC
# MAGIC     if max (data) == min(data):
# MAGIC         best_n_components = 1
# MAGIC     else: 
# MAGIC         gmm_result = []
# MAGIC         for n_components in range(1,4):
# MAGIC             gmm = GaussianMixture(n_components).fit(data.reshape(-1, 1))
# MAGIC             gmm_result.append(gmm.bic(data.reshape(-1, 1)))
# MAGIC         best_n_components = gmm_result.index(min(gmm_result))+1
# MAGIC     
# MAGIC     # use the selected the best n_numbers to split the data base on GMM algo
# MAGIC     gmm = GaussianMixture(best_n_components).fit(data.reshape(-1, 1))
# MAGIC     
# MAGIC     # if there is only one Gaussian distribution, the split will just take the mean of distribution 
# MAGIC     if best_n_components == 1:
# MAGIC         gmm_splits = gmm.means_[0] 
# MAGIC     else:
# MAGIC     # if there is more than two Gaussian distributions, the splits will take the mean and weight and different distribution 
# MAGIC         gmm_df = pd.DataFrame({'weight':gmm.weights_,'means':gmm.means_.reshape(best_n_components,)})
# MAGIC         gmm_df = gmm_df. sort_values(by='means').reset_index(drop=True)
# MAGIC
# MAGIC         for i in range(len(gmm_df)-1):
# MAGIC         # the split will consider the means and weight of different distributions 
# MAGIC             gmm_df.loc[i, 'splits'] = gmm_df.loc[i, 'means'] + (gmm_df.loc[i+1, 'means'] -gmm_df.loc[i, 'means'])*gmm_df.loc[i, 'weight']/(gmm_df.loc[i, 'weight']+gmm_df.loc[i+1, 'weight'])
# MAGIC         
# MAGIC         gmm_splits = list(gmm_df['splits'].dropna().values)
# MAGIC     # Return the gmm_splits    
# MAGIC     return gmm_splits  
# MAGIC
# MAGIC def get_gmm_splits_number (data, variable): 
# MAGIC     
# MAGIC     """
# MAGIC     Function that splits the data base on the GMM algo and return the number of gaussian distributions.  
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed to get peaks
# MAGIC     variable: str - the variable that for peak detection
# MAGIC     """
# MAGIC     # extract key_feature data 
# MAGIC     data = np.array(data[variable])
# MAGIC     
# MAGIC     # select the best n_numbers, looping from 1 to 3 >> edge case that max(data) == min (data), which means the variable is consant, then the gmm_splits_number is defaluted to be 1 culster only. 
# MAGIC     #compare with n_components best fit the data, ranging from 1 to 3.
# MAGIC     # based on the matric of gmm.bic Bayesian information criterion (BIC):This criterion gives us an estimation on how much is good the GMM in terms of predicting the data we actually have. The lower is the BIC, the better is the model to actually predict the data we have, and by extension, the true, unknown, distribution.
# MAGIC     
# MAGIC     if max (data) == min(data):
# MAGIC         gmm_splits_number = 1
# MAGIC     else: 
# MAGIC         gmm_result = []
# MAGIC         for n_components in range(1,4):
# MAGIC             gmm = GaussianMixture(n_components).fit(data.reshape(-1, 1))
# MAGIC             gmm_result.append(gmm.bic(data.reshape(-1, 1)))
# MAGIC         gmm_splits_number = gmm_result.index(min(gmm_result))+1
# MAGIC     #return the number of gaussian distributions.
# MAGIC     return gmm_splits_number
# MAGIC
# MAGIC def DTreeReg_gmm_splits(data, target, variable):
# MAGIC     """
# MAGIC     Function that return tree spltis 
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed for the algo. 
# MAGIC     target: str - the 'dependent' variable in terms of unsupervised learning. 
# MAGIC     variable: str - the 'independent' variable in terms of unsupervised learning. 
# MAGIC     """
# MAGIC     
# MAGIC     # DTreeReg_gmm_splits logic: 
# MAGIC     # if there are only ONE gaussian (gmm_splits_number ==1)detected, use unsupervised Tree Regression to split the dependent variable into two sets with max_depth =1
# MAGIC     # if there are none or more than 2 gaussian detected, use unsupervised Tree Regression to split the dependent variable into more than two sets with max_depth =2 
# MAGIC     
# MAGIC     gmm_splits_number = get_gmm_splits_number(data, variable)
# MAGIC     
# MAGIC     if gmm_splits_number ==1:
# MAGIC         DTreeReg = DecisionTreeRegressor(max_depth =1, min_samples_split = .2)
# MAGIC     else:
# MAGIC         DTreeReg = DecisionTreeRegressor(max_depth =2, min_samples_split = .2)        
# MAGIC         
# MAGIC     fit = DTreeReg.fit(data[[variable]],data[target])
# MAGIC     tree_splits = np.sort(DTreeReg.tree_.threshold[(DTreeReg.tree_.threshold >= 0)])
# MAGIC     
# MAGIC     return tree_splits
# MAGIC
# MAGIC # The following section is for auto_visaulizatin DTreeReg_peak_splits testing
# MAGIC def get_peaks(data, variable):    
# MAGIC     """
# MAGIC     Function that return peaks
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed to get peaks
# MAGIC     variable: str - the variable that for peak detection
# MAGIC     """
# MAGIC     # create histograme based on the input key_feature
# MAGIC     hist, bin_edges = np.histogram(data[variable],10)    
# MAGIC     bin_edges = bin_edges[1:]
# MAGIC     
# MAGIC     # Length of the available
# MAGIC     Length = len(data[variable])
# MAGIC  
# MAGIC     # peaks detection logic: 
# MAGIC     # 1) divide the dataset into 10 bins 
# MAGIC     # 2) for any bin that has more than 10% of dataset, is defined as peak (return as index) 
# MAGIC     # 3) return the first element of the peak_bin element as the peak     
# MAGIC     peaks, _ = find_peaks(hist, height=(Length*0.1,Length))
# MAGIC     
# MAGIC     # if there is peaks detected, return all the peaks, if none, return none
# MAGIC     if len(peaks)>0:        
# MAGIC         peaks = bin_edges[peaks]
# MAGIC     else:
# MAGIC         peaks = []
# MAGIC     return peaks
# MAGIC
# MAGIC def DTreeReg_peak_splits(data,target,variable):    
# MAGIC     """
# MAGIC     Function that return tree spltis 
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed for the algo. 
# MAGIC     target: str - the 'dependent' variable in terms of unsupervised learning. 
# MAGIC     variable: str - the 'independent' variable in terms of unsupervised learning. 
# MAGIC     """
# MAGIC     # call get_peaks function to get peaks detected from the independent variable
# MAGIC     peaks = get_peaks(data,variable)     
# MAGIC     peaks_number = len(peaks)
# MAGIC     
# MAGIC     # DTreeReg_peak_splits logic: 
# MAGIC     # if there are only ONE peak (peaks_number ==1)detected from get_peaks, use unsupervised Tree Regression to split the dependent variable into two sets with max_depth =1
# MAGIC     # if there are none or more than 2 peaks detected, use unsupervised Tree Regression to split the dependent variable into more than two sets with max_depth =2 
# MAGIC            
# MAGIC     if peaks_number ==1:
# MAGIC         DTreeReg = DecisionTreeRegressor(max_depth =1, min_samples_split = .2)
# MAGIC     else:
# MAGIC         DTreeReg = DecisionTreeRegressor(max_depth =2, min_samples_split = .2)
# MAGIC     # fit the Tree regression data    
# MAGIC     fit = DTreeReg.fit(data[[variable]],data[target])
# MAGIC     tree_splits = np.sort(DTreeReg.tree_.threshold[(DTreeReg.tree_.threshold >= 0)])    
# MAGIC     return tree_splits
# MAGIC
# MAGIC # The following section is for auto_visaulizatin Plotting
# MAGIC
# MAGIC def BW_plot (data,target,variable,tree_splits):
# MAGIC     """
# MAGIC     Function that return tree spltis 
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed for the algo. 
# MAGIC     target: str - the 'dependent' variable in terms of unsupervised learning. 
# MAGIC     variable: str - the 'independent' variable in terms of unsupervised learning.
# MAGIC     tree_splits: list - the way the unsupervised tree algo. split the 'independent' variable.
# MAGIC     """
# MAGIC     # create empty lists to store data    
# MAGIC     conditions = [None]*(len(tree_splits)+1)
# MAGIC     cats = [None]*(len(tree_splits)+1)    
# MAGIC     #create a new dataframe for target and key_feature only 
# MAGIC     visual_df = data[[target,variable]]
# MAGIC     
# MAGIC     # BW splits logic: 
# MAGIC     # 1) If there is only one tree_splits (len(tree_splits)==1), split the data into two category base on the tree_splits
# MAGIC     # 2) If there is more than one tree_splits, split the data into multiple datasets base on the tree_splits
# MAGIC     
# MAGIC     # Helper function to detect whether certain category is outlier with total datapoint less than 10%
# MAGIC     # Reason, becasue the minimum split of trees is 10% 
# MAGIC     def outlier_detect(visual_df, condition):
# MAGIC         percentage = len(visual_df[condition])/len(visual_df)
# MAGIC         if percentage<0.1:
# MAGIC             outlier = ' (Outlier)'
# MAGIC         else: 
# MAGIC             outlier=''
# MAGIC         return outlier 
# MAGIC     
# MAGIC     if len(tree_splits)==1:        
# MAGIC         conditions[0] = (visual_df[variable] < tree_splits[0])
# MAGIC         cats[0] = '<'+str(round(tree_splits[0],2))
# MAGIC         conditions[1] = (visual_df[variable] >= tree_splits[0])
# MAGIC         cats[1] = '>='+str(round(tree_splits[0],2))    
# MAGIC     else:    
# MAGIC
# MAGIC         for i in range(len(tree_splits)+1):
# MAGIC             if i == 0:                
# MAGIC                 condition = visual_df[variable] < tree_splits[i] 
# MAGIC                 #print(condition)
# MAGIC                 conditions[i] = (condition)
# MAGIC                 outlier = outlier_detect(visual_df, condition)
# MAGIC                 cats[i] = '<'+str(round(tree_splits[i],2))+ outlier 
# MAGIC             elif i == len(tree_splits):                
# MAGIC                 condition = visual_df[variable] > tree_splits[-1]
# MAGIC                 conditions[i] = (condition)
# MAGIC                 outlier = outlier_detect(visual_df, condition)
# MAGIC                 cats[i]  = '>'+str(round(tree_splits[-1],2))+outlier                 
# MAGIC             else:
# MAGIC                 condition = (visual_df[variable] < tree_splits[i]) & (visual_df[variable] >= tree_splits[i-1])
# MAGIC                 conditions[i] = (condition)
# MAGIC                 outlier = outlier_detect(visual_df, condition)
# MAGIC                 cats[i] = str(round(tree_splits[i-1],2))+'-'+str(round(tree_splits[i],2))+outlier
# MAGIC                 
# MAGIC     # conditions_result is used to categoraize targets based on cats setting
# MAGIC     conditions_result = np.select(conditions, cats)
# MAGIC     visual_df.insert(2,'Category',conditions_result) 
# MAGIC     
# MAGIC     ax = sns.boxplot(x='Category', y= target, data=visual_df, order = cats).set(title=variable)     
# MAGIC     # calculate and print out the average of each category 
# MAGIC     avg_df = visual_df.groupby('Category')[target].mean().reindex(cats).reset_index()
# MAGIC     print(avg_df)
# MAGIC     
# MAGIC def feature_distribution_splits(data,feature,splits):
# MAGIC     """
# MAGIC     Function that plot the histogram of interested feature with splits or peaks return from other function. 
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed for the algo. 
# MAGIC     feature: str - the interested feature to plot as histogram
# MAGIC     splits: list - the splits / peaks need to be visualized from the histogram graph. 
# MAGIC     """    
# MAGIC     plt.figure()
# MAGIC     data[feature].hist(bins=100)
# MAGIC     
# MAGIC     for xc in splits:
# MAGIC         plt.axvline(x=xc,color='r',ls='--')
# MAGIC         
# MAGIC     plt.title(feature) 
# MAGIC     plt.show()
# MAGIC     
# MAGIC def feature_distribution(data,feature):
# MAGIC     """
# MAGIC     Function that plot the histogram of interested feature. 
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data needed for the algo. 
# MAGIC     feature: str - the interested feature to plot as histogram 
# MAGIC     """
# MAGIC     plt.figure()
# MAGIC     data[feature].hist(bins=100)            
# MAGIC     plt.title(feature) 
# MAGIC     plt.show()
# MAGIC
# MAGIC def time_series_plot (data,feature):    
# MAGIC     """
# MAGIC     Function that plot the time-series plot of interested feature with rolling average of the last 4 hours
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data for plotting. 
# MAGIC     feature: str - the interested feature to plot  the time-series
# MAGIC     """
# MAGIC     df = pd.DataFrame(data[[feature,'time_bucket_local']])
# MAGIC     new_col_name = feature + ' moving average (24hr)'
# MAGIC     df[new_col_name] = df[feature].rolling(48).mean()
# MAGIC     
# MAGIC     #Addressing overlare axis issues pt1
# MAGIC     df['time_bucket_local'] = pd.to_datetime(df['time_bucket_local'])
# MAGIC    #n = len(df['time_bucket_local']) // min(int(n_month),8)
# MAGIC     n = len(df['time_bucket_local']) // 8
# MAGIC
# MAGIC     # set figure size
# MAGIC     plt.figure( figsize = ( 12, 5))
# MAGIC     
# MAGIC     # plot a simple time series plot
# MAGIC     # using seaborn.lineplot()
# MAGIC     sns.lineplot(x = 'time_bucket_local',
# MAGIC              y = feature,
# MAGIC              data = df,
# MAGIC              label =feature)
# MAGIC   
# MAGIC     # plot using rolling average
# MAGIC     sns.lineplot(x = 'time_bucket_local', 
# MAGIC                  y = new_col_name,
# MAGIC                  data = df,
# MAGIC                  label = 'Average (24hrs)')
# MAGIC     #Creating readable axis part 2
# MAGIC     plt.xticks(df['time_bucket_local'][::n], rotation=45)
# MAGIC
# MAGIC
# MAGIC # The following section is for machine leanring model selection, feature importance calculating
# MAGIC def ranking_models(data,target,variables,model_dict):
# MAGIC     """
# MAGIC     Function that select & return the most accurated ML that could capture the relationship beween the dependent vs. independent variables. 
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data for ML training. 
# MAGIC     target: str - dependent variable (y)
# MAGIC     variables: list - list of independent variables (X)
# MAGIC     model_dict: dictinary - dictionary of sets of machine leanring model candidates    
# MAGIC     """
# MAGIC     X = data[variables]
# MAGIC     y=  data[target]
# MAGIC     x_train, x_test, y_train, y_test=train_test_split(X,y,test_size=0.2)
# MAGIC     models =[item for item in model_dict.items()]    
# MAGIC     mape = []
# MAGIC     mse=[]
# MAGIC     names =[]     
# MAGIC     for name, model in models:
# MAGIC         kfold = model_selection.KFold(n_splits=5, random_state=7,shuffle=True)
# MAGIC         mape_results = model_selection.cross_val_score(model, x_train, y_train, cv=kfold,scoring = 'neg_mean_absolute_percentage_error')
# MAGIC         mse_results = model_selection.cross_val_score(model, x_train, y_train, cv=kfold,scoring = 'neg_root_mean_squared_error')
# MAGIC         mape.append(mape_results.mean())
# MAGIC         mse.append(mse_results.mean())
# MAGIC         names.append(name)
# MAGIC     
# MAGIC     mape = list(np.array(mape)*(-1)*100)
# MAGIC     mse = list(np.array(mse)*(-1))
# MAGIC     
# MAGIC     #accuracy_df = pd.DataFrame({'model':names, 'abs_mape_%':mape,'cv':cv})
# MAGIC     accuracy_df = pd.DataFrame({'model':names, 'abs_mape_%':mape,'abs_mse':mse})
# MAGIC     #abs_mape the lower the better
# MAGIC     accuracy_df  = accuracy_df.sort_values(by='abs_mape_%',ascending=True)
# MAGIC     
# MAGIC     selected_model = accuracy_df.iloc[0].model
# MAGIC     
# MAGIC     #print('Selected model is: '+ selected_model)
# MAGIC     #print()
# MAGIC    # print('Please wait for the modelling results...')
# MAGIC     
# MAGIC     return selected_model
# MAGIC
# MAGIC def normalize(data):
# MAGIC     """
# MAGIC     Function that normalize the data for linear regression based model 
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data for ML training. 
# MAGIC     """       
# MAGIC     normalized_data = data.copy()
# MAGIC     for feature_name in normalized_data.columns:
# MAGIC         max_value = normalized_data[feature_name].max()
# MAGIC         min_value = normalized_data[feature_name].min()
# MAGIC         normalized_data[feature_name] = (normalized_data[feature_name] - min_value) / (max_value - min_value)
# MAGIC    
# MAGIC     normalized_data = normalized_data.dropna(how='all',axis='columns')
# MAGIC     return normalized_data
# MAGIC
# MAGIC def lasso_regression_feature_importance(data, target, variables):
# MAGIC     """
# MAGIC     Function that calculate the feature importances based on the k-fold lasso_regression
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data for ML training. 
# MAGIC     target: str - dependent variable (y)
# MAGIC     variables: list - list of independent variables (X)
# MAGIC     """
# MAGIC     # normalized data 
# MAGIC     normalized_data = normalize(data)
# MAGIC     # remaining featues 
# MAGIC     remaining_features = normalized_data.columns.to_list()
# MAGIC     remaining_features.remove(target)    
# MAGIC     
# MAGIC     # Retrieve the target and independent variables 
# MAGIC     y= normalized_data[target]
# MAGIC     X= normalized_data[remaining_features]
# MAGIC     
# MAGIC         
# MAGIC     kf = KFold(n_splits=10)
# MAGIC     lr = Lasso(alpha=0.01)
# MAGIC     
# MAGIC     # Calculate the feature importance
# MAGIC     feature_importance_dict={}
# MAGIC     count = 1     
# MAGIC     for train, test in kf.split(X,y):
# MAGIC         lr.fit(X.iloc[train], y.iloc[train])
# MAGIC         lr_coef_list=lr.coef_.tolist()
# MAGIC         feature_importance=pd.DataFrame({'feature':remaining_features,'coefficient':lr_coef_list})
# MAGIC         feature_importance['feature_importance']=abs(feature_importance['coefficient'])
# MAGIC         feature_importance_dict[count]=feature_importance[['feature_importance','feature']]
# MAGIC         count += 1
# MAGIC     feature_importance_all=pd.concat(feature_importance_dict.values())
# MAGIC     
# MAGIC     feature_importance_all =feature_importance_all.groupby('feature').mean().sort_values('feature_importance',ascending= False).reset_index()
# MAGIC     
# MAGIC     feature_importance_all['index'] = -feature_importance_all['feature_importance']
# MAGIC     
# MAGIC     return feature_importance_all
# MAGIC
# MAGIC def ridge_regression_feature_importance(data, target, variables):
# MAGIC     
# MAGIC     """
# MAGIC     Function that calculate the feature importances based on the k-fold ridge_regression
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data for ML training. 
# MAGIC     target: str - dependent variable (y)
# MAGIC     variables: list - list of independent variables (X)
# MAGIC     """
# MAGIC     # normalized data 
# MAGIC     normalized_data = normalize(data)
# MAGIC     # remaining featues 
# MAGIC     remaining_features = normalized_data.columns.to_list()
# MAGIC     remaining_features.remove(target)    
# MAGIC     
# MAGIC     # Retrieve the target and independent variables 
# MAGIC     y= normalized_data[target]
# MAGIC     X= normalized_data[remaining_features]
# MAGIC     
# MAGIC         
# MAGIC     kf = KFold(n_splits=10)
# MAGIC     rr = Ridge(alpha=0.01)
# MAGIC     
# MAGIC     # Calculate the feature importance
# MAGIC     feature_importance_dict={}
# MAGIC     count = 1     
# MAGIC     for train, test in kf.split(X,y):
# MAGIC         rr.fit(X.iloc[train], y.iloc[train])
# MAGIC         rr_coef_list=rr.coef_.tolist()
# MAGIC         feature_importance=pd.DataFrame({'feature':remaining_features,'coefficient':rr_coef_list})
# MAGIC         feature_importance['feature_importance']=abs(feature_importance['coefficient'])
# MAGIC         feature_importance_dict[count]=feature_importance[['feature_importance','feature']]
# MAGIC         count += 1
# MAGIC     feature_importance_all=pd.concat(feature_importance_dict.values())
# MAGIC     
# MAGIC     feature_importance_all =feature_importance_all.groupby('feature').mean().sort_values('feature_importance',ascending= False).reset_index()
# MAGIC     
# MAGIC     feature_importance_all['index'] = -feature_importance_all['feature_importance']
# MAGIC     
# MAGIC     return feature_importance_all
# MAGIC
# MAGIC def linear_regression_feature_importance(data, target, variables):
# MAGIC     
# MAGIC     """
# MAGIC     Function that calculate the feature importances based on the k-fold linear_regression
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data for ML training. 
# MAGIC     target: str - dependent variable (y)
# MAGIC     variables: list - list of independent variables (X)
# MAGIC     """
# MAGIC     # normalized data 
# MAGIC     normalized_data = normalize(data)
# MAGIC     # remaining featues 
# MAGIC     remaining_features = normalized_data.columns.to_list()
# MAGIC     remaining_features.remove(target)    
# MAGIC     
# MAGIC     # Retrieve the target and independent variables 
# MAGIC     y= normalized_data[target]
# MAGIC     X= normalized_data[remaining_features]
# MAGIC     
# MAGIC         
# MAGIC     kf = KFold(n_splits=10)
# MAGIC     lr = LinearRegression()
# MAGIC     
# MAGIC     # Calculate the feature importance
# MAGIC     feature_importance_dict={}
# MAGIC     count = 1     
# MAGIC     for train, test in kf.split(X,y):
# MAGIC         lr.fit(X.iloc[train], y.iloc[train])
# MAGIC         lr_coef_list=lr.coef_.tolist()
# MAGIC         feature_importance=pd.DataFrame({'feature':remaining_features,'coefficient':lr_coef_list})
# MAGIC         feature_importance['feature_importance']=abs(feature_importance['coefficient'])
# MAGIC         feature_importance_dict[count]=feature_importance[['feature_importance','feature']]
# MAGIC         count += 1
# MAGIC     feature_importance_all=pd.concat(feature_importance_dict.values())
# MAGIC     
# MAGIC     feature_importance_all =feature_importance_all.groupby('feature').mean().sort_values('feature_importance',ascending= False).reset_index()
# MAGIC     
# MAGIC     feature_importance_all['index'] = -feature_importance_all['feature_importance']
# MAGIC     
# MAGIC     return feature_importance_all
# MAGIC
# MAGIC def random_forest_feature_selection(data, target, variables):
# MAGIC     """
# MAGIC     Function that calculate the feature importances based on the k-fold random forest regression
# MAGIC     Parameters:
# MAGIC     data: pd.DataFrame - dataframe containing the data for ML training. 
# MAGIC     target: str - dependent variable (y)
# MAGIC     variables: list - list of independent variables (X)
# MAGIC     """
# MAGIC     
# MAGIC     X = data[variables]
# MAGIC     y=  data[target]
# MAGIC     kf = KFold(n_splits=10)
# MAGIC     rf = RandomForestRegressor(n_estimators=50) 
# MAGIC     
# MAGIC     # Calculate the feature importance
# MAGIC     feature_importance_dict={}
# MAGIC     count = 1
# MAGIC     for train, test in kf.split(X, y):
# MAGIC         rf.fit(X.iloc[train], y.iloc[train])
# MAGIC         feature_importance = pd.DataFrame(variables,rf.feature_importances_).reset_index()   
# MAGIC         feature_importance =feature_importance.sort_values('index',ascending=False).reset_index().rename({'index':'feature_importance', 0:'feature'},axis=1).reset_index()  
# MAGIC         feature_importance_dict[count]=feature_importance[['index','feature_importance','feature']]
# MAGIC         count += 1
# MAGIC     feature_importance_all=pd.concat(feature_importance_dict.values())
# MAGIC     feature_importance_all =feature_importance_all.groupby('feature').mean().sort_values('index',ascending=True).reset_index()
# MAGIC     return feature_importance_all
# MAGIC
# MAGIC def feature_importance (feature_importance_all):    
# MAGIC     #print(feature_importance_all)
# MAGIC     #plt.figure( figsize = ( 12, 5))
# MAGIC     feature_importance_allt10 = feature_importance_all.head(10)
# MAGIC     #changing to all10 to limit to top 10
# MAGIC     max_index = max(feature_importance_allt10['index'])
# MAGIC     plt.figure(figsize=(10,10))
# MAGIC     plt.suptitle('Feature Importance')
# MAGIC     plt.barh(feature_importance_allt10['feature'],max_index-feature_importance_allt10['index'])
# MAGIC     plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #The "000 Reset Filters?" command will remove all commands with the exception of the site and machine commands. Use this command if it is believed that the notebook is malfunctioning due to hidden selections in product codes or variables

# COMMAND ----------

#RH- Creating this command to perform a reset action if the user can't get rid of error producing product codes or variables. All filters except 2 should be removed
dbutils.widgets.dropdown('000: Reset Filters?', 'No', ['Yes', 'No'])
work = 2
choosen = dbutils.widgets.get('000: Reset Filters?')


if choosen == 'Yes':
    try:
        dbutils.widgets.remove('02: Select prod_codes?')
    except:
        print('Select prod_codes not active')
    try:
        dbutils.widgets.remove('02.5: Error 02')
    except:
        print('Error 02 Not Active')
    try:
        dbutils.widgets.remove('03: Start Date?')
    except:
        print('Start Date not active')
    try:
        dbutils.widgets.remove('03.5: Error 03')
    except:
        print('Error 03 Not Active')
    try:
        dbutils.widgets.remove('04: End Date?')
    except:
        print('End Date not active')
    try:
        dbutils.widgets.remove('04.5: Error 04')
    except:
        print('Error 04 Not Active')
    try:
        dbutils.widgets.remove('05: Select Target?')
    except:
        print('Select target not active')
    try:
        dbutils.widgets.remove('05.5: Error 05')
    except:
        print('Error 05 Not Active')
    try:
        dbutils.widgets.remove('06: Provide Min Target Value?')
    except:
        print('Min Target Value not active')
    try:
        dbutils.widgets.remove('06.5: Error 06')
    except:
        print('Error 06 Not Active')
    try:    
        dbutils.widgets.remove('07: Provide Max Target Value?')
    except:    
        print('Max Target Value not active')
    try:
        dbutils.widgets.remove('07.5: Error 07')
    except:
        print('Error 07 Not Active')
    try:    
        dbutils.widgets.remove('08: Remove Variables?')
    except:
        print('Remove Variables not active')
    try:
        dbutils.widgets.remove('08.5: Error 08')
    except:
        print('Error 08 Not Active')
    try:    
        dbutils.widgets.remove('09: Run Feature Selection?')
    except:
        print('Feature Selection not active')
    try:
        dbutils.widgets.remove('09.5: Error 09')
    except:
        print('Error 09 Not Active')
    try:    
        dbutils.widgets.remove('10: Select Variables?')
    except:
        print('Variables not active')
    try:
        dbutils.widgets.remove('10.5: Error 10')
    except:
        print('Error 10 Not Active')
    dbutils.widgets.dropdown('000: Reset Filters?', 'No', ['Yes', 'No'])

# COMMAND ----------

# MAGIC %md
# MAGIC #The following directions presume all filters or widgets are present and showing. If they are not, please click the “Run All” button in the top right corner and follow the below directions after step 1.

# COMMAND ----------

# MAGIC %md
# MAGIC #1.	 Clear all values in filter “04: Select Target?” and “07: Select Variables?”

# COMMAND ----------

# The following section is for Results only, please dont' change the cell order! 
# create widge for Site Selection: 
create_widget('00: Select Site?', site_machine_df, 'site', 'dropdown')
#
selector = VarianceThreshold()

# COMMAND ----------

# MAGIC %md
# MAGIC #2.	 Select the abbreviation for the site (cg = Cape Girardeau) in filter "00: Select Site?”

# COMMAND ----------

# Retrieve the selected site
select_site = dbutils.widgets.get('00: Select Site?')
# Filter the machine_df by select_site
machine_df = site_machine_df[site_machine_df['site'] == select_site]

#print machine and time taken
print("Selected ", select_site, " site at ", datetime.datetime.now(pytz.timezone('America/New_York')))
# create widge for Machine Selection: 
create_widget('01: Select Machine?', machine_df, 'machines', 'dropdown')

dbutils.widgets.remove('000: Reset Filters?')


# COMMAND ----------

# MAGIC %md
# MAGIC #3.	 Select the abbreviation for the machine in filter "01: Select Line?”. Please be patient and wait for the data to load, as it may take a couple of seconds. Selecting the next filter too soon may cause it to not be registered

# COMMAND ----------

# Retrieve the selected machine select_machine
select_machine = dbutils.widgets.get('01: Select Machine?') 
# Use the helper function to create the sql
sql = creat_sql(select_site, select_machine)
# connect to our database with created sql
raw_data =  spark.sql(sql).toPandas()
datatest = raw_data
print(datatest.shape)
"""
Sort data by time-series ascending order 
"""
raw_data = raw_data.sort_values (by=['time_bucket_local'], ascending=True)
#print site
print("Selected ", select_machine, " machine at ", datetime.datetime.now(pytz.timezone('America/New_York')))
"""
Fill Forward the raw_data before extracting the historical data 
"""
raw_data = raw_data.ffill(axis = 0)

# get the prod_code >> Sorted by descending order (to modify with datapoints)
prod_code_df = prod_codes_sorting(raw_data)

# Widgets multiselection of prod_code
create_widget('02: Select prod_codes?', prod_code_df, 'prod_code', 'multiselect')


#RH I am removing old values 
#if select_prod_codes != ['']:
 #   dbutils.widgets.remove('03: Select prod_codes?'),
dbutils.widgets.dropdown('000: Reset Filters?', 'No', ['Yes', 'No'])

# COMMAND ----------

# MAGIC %md
# MAGIC #4.	 Select the products to be examined in “02: Select prod_codes?”. A “Last Comand Completed” response will trigger in the upper right hand of the corner of notebook each time a product code is selected. Ignore this and select all products needed.

# COMMAND ----------

if work  > 1:
    try:         
        
        # Retrieve the selected prod_codes select_prod_codes, then convert to a list
        select_prod_codes = dbutils.widgets.get('02: Select prod_codes?').split(',')

        if select_prod_codes == ['']:
            print('No product codes selected at', datetime.datetime.now(pytz.timezone('America/New_York')))
            dbutils.widgets.text('02.5: Error 02', "No prod codes selected")
        else:

            # Get the training data 
            data = get_prod_data(raw_data,select_prod_codes)
            #print(data.shape)
            #Printing product codes
            print("Selected ", select_prod_codes, " product codes at ", datetime.datetime.now(pytz.timezone('America/New_York')))
            
            #The following section is used for outlier / columns removal
            
            
            # Remove messy columns: 
            #if more than 30% of specific columns are missing, 
            for col in data.columns:
                if data[col].count() <= data.shape[0]*0.3:
                    data = data.drop(col, axis=1)

            print('You have ', data.shape[0], ' samples of data and ', data.shape[1],' variables.')
            #Giving data status
            if data.shape[0] == 0:
                print('Your model has no data and will fail')
            elif data.shape[0] < data.shape[1] and data.shape[0] > 0:
                print('You do not have enough data to run a successful model. It is recommended to loosen constraints')

            # Get the time series data 
            time_series_data = get_time_series_data (data,select_prod_codes)

            try:
                dbutils.widgets.remove('02.5: Error 02')
            except:
                print(' ') #printing empty so something is there to prevent error on try
            #print(time_series_data.shape)

        # create widge to enable users to provide input for minimum value for target valuable 
            dbutils.widgets.text('03: Start Date?', "2022-01-01")
            work = 3
    except:
        work = 2
        dbutils.widgets.text('02.5: Error 02', "Filter 02 failed")
else:
    print('Previous widget failed. Not running this widget')



# COMMAND ----------

# MAGIC %md
# MAGIC #5.	Select the start date of the period you want to look at in filter “02: Start Date?”. The user can go back 18 months

# COMMAND ----------

if work > 2:
    try:        
        
        startdate = dbutils.widgets.get('03: Start Date?')

        # Retrieve the historical data 
        #data = historical_data (raw_data,int(n_month) ) 

        raw_data2 = data[data['time_bucket_local'] >= startdate]
        print("Selected ", startdate, " start date at ", datetime.datetime.now(pytz.timezone('America/New_York')))
        print('You have ', raw_data2.shape[0], ' samples of data and ', raw_data2.shape[1],' variables.')
        #print(np.min(raw_data['time_bucket_local']))
        #print(np.min(raw_data2['time_bucket_local']))

        dbutils.widgets.text('04: End Date?', "2025-01-01")
        try:
            dbutils.widgets.remove('03.5: Error 03')
        except:
            print(' ') #printing empty so something is there to prevent error on try
        work = 4
    except:
        work = 3
        dbutils.widgets.text('03.5: Error 03', "Filter 03 failed")

else:
    print('Previous widget failed. Not running this widget')

# COMMAND ----------

# MAGIC %md
# MAGIC #6.	Select the end date of the period you want to look at in filter “02: End Date?”. The data is updated daily

# COMMAND ----------

if work > 3:
    try: 


        enddate = dbutils.widgets.get('04: End Date?')
        #Set end date of data
        training_data = raw_data2[raw_data2['time_bucket_local'] <= enddate]


        # Retrieve the historical data 
        #data = historical_data (raw_data,int(n_month) ) 


        #Printing time selected
        print("Selected ", enddate, " end date at ", datetime.datetime.now(pytz.timezone('America/New_York')))

        if training_data.shape[0] == 0:
            print('Your model has no data and will fail')
        elif training_data.shape[0] < training_data.shape[1] and training_data.shape[0] > 0:
            print('You do not have enough data to run a successful model. It is recommended to loosen constraints')

                #The following section is used for outlier / columns removal
        # get rid of redundant columns     
        training_data = data.iloc[:,5:]    

        #Getting rid of variables with 0 variance
        selector.fit(training_data)
        training_data = training_data.iloc[:,selector.get_support([training_data.columns])]


        # Remove all columns are 0 
        #training_data = training_data.loc[:, (training_data != 0).any(axis=0)]
        # Remove outliers by quantile 

        # training_data = remove_outlier(training_data)
        #print('Shape of filtered raw dataset')
        print('You have ', training_data.shape[0], ' samples of data and ', training_data.shape[1],' variables.')
        #Giving data status

        #time_series_data = get_time_series_data (Training_data_data,select_prod_codes)
            

        # Get the features 
        column_df = pd.DataFrame (training_data.columns, columns = ['features'])

            

        # create widge for Target Feature: 
        create_widget('05: Select Target?', column_df, 'features', 'dropdown')
        try:
            dbutils.widgets.remove('04.5: Error 04') 
        except:
            print(' ') #printing empty so something is there to prevent error on try
        work = 5
    except:
        work = 4
        dbutils.widgets.text('04.5: Error 04', "Filter 04 failed")
else:
    print('Previous widget failed. Not running this widget')

# COMMAND ----------

# MAGIC %md
# MAGIC #7.	 Enter the target of the model in filter “05: Select Target?”. This should a meaningful value such as a cost to be reduced or a quality metric to be increased. A time series graph and a histogram should appear in results once this is run.

# COMMAND ----------

if work > 4:
    try: 

        # Retrieve the target
        target = dbutils.widgets.get('05: Select Target?')
        # plot the time-series of the target variable
        #Printing target
        print("Selected ", target, " target at ", datetime.datetime.now(pytz.timezone('America/New_York')))
        time_series_plot (time_series_data,target)
        feature_distribution(training_data,target)

        # create widge to enable users to provide input for minimum value for target valuable 
        dbutils.widgets.text('06: Provide Min Target Value?', "0")
        try:
            dbutils.widgets.remove('05.5: Error 05')
        except:
            print(' ') #printing empty so something is there to prevent error on try
        work = 6
        
    except:
        work = 5
        dbutils.widgets.text('05.5: Error 05', "Filter 05 failed")
else:
    print('Previous widget failed. Not running this widget')

# COMMAND ----------

# MAGIC %md
# MAGIC #8. Use the previous results and the user’s best judgement to remove unreasonably low datapoints by setting a cutoff in the “06: Provide Min Target Value?” filter. This cutoff is intended to remove data errors and should facilitated by the user’s domain knowledge of what is unlikely to be accurate. If there are no data points to be removed, simply set this value to be lower than all data points

# COMMAND ----------

if work > 5:
    try: 
        min_target = dbutils.widgets.get('06: Provide Min Target Value?')
        # Don't change the original training_data 
        #Printing minimum cut off
        print("Selected ", min_target, " minimum cut off at ", datetime.datetime.now(pytz.timezone('America/New_York')))

        if float(min_target) > np.max(training_data[target]) and float(np.max(training_data[target])) is not None:
            print('The selected min_target of ', float(min_target), 'is greater than your max data of ',float(np.max(training_data[target])), '. Choose a smaller min.' )
            dbutils.widgets.text('06.5: Error 06', "Min has eliminated data")
        else:

            filter_training_datai = training_data[training_data[target]>=float(min_target)]
            #feature_distribution(filter_training_data,target) 

            print('You have ', filter_training_datai.shape[0], ' samples of data and ', filter_training_datai.shape[1],' variables.')
            #Giving data status
            if filter_training_datai.shape[0] == 0:
                print('Your model has no data and will fail')
            elif filter_training_datai.shape[0] < filter_training_datai.shape[1] and filter_training_datai.shape[0] > 0:
                print('You do not have enough data to run a successful model. It is recommended to loosen constraints')
                

            filter_datai = time_series_data[time_series_data[target]>=float(min_target)]
            dbutils.widgets.text('07: Provide Max Target Value?', "10000")
            try:
                dbutils.widgets.remove('06.5: Error 06')
            except:
                print(' ') #printing empty so something is there to prevent error on try
            work = 7

    except:
        work = 6
        dbutils.widgets.text('06.5: Error 06', "Filter 06 failed")
else:
    print('Previous widget failed. Not running this widget')

# COMMAND ----------

# MAGIC %md
# MAGIC #9.	 This filter is the same as the previous except applied to the max. The “07: Provide Max Target Value” cutoff is a ceiling, and if no datapoints are to be removed than it should be set at a higher value than all datapoints. This data will trigger a time series

# COMMAND ----------

if work > 6:
    try: 
        max_target = dbutils.widgets.get('07: Provide Max Target Value?')
        #Printing maximum cut off
        print("Selected ", max_target, " maximum cut off at ", datetime.datetime.now(pytz.timezone('America/New_York')))

        if float(max_target) <= np.min(filter_training_datai[target]) and float(np.min(filter_training_datai[target])) is not None:
            print('The selected max_target of ', float(max_target), 'is less than your min data of ',float(np.min(filter_training_datai[target])), '. Choose a larger max.' )
            dbutils.widgets.text('07.5: Error 07', "Max has eliminated data")
        else:
            filter_training_data = filter_training_datai[filter_training_datai[target]<=float(max_target)]

            filter_data = filter_datai[filter_datai[target]<float(max_target)]
            print('The time-series of Target variable')

            filter_training_data = filter_training_data.dropna(axis = 0)


            #This is visualisation portion of work to decide whether filters were effective or no
            #print(filter_data.shape)
            time_series_plot (filter_data,target)
            feature_distribution(filter_data,target)

            print('You have ', filter_training_data.shape[0], ' samples of data and ', filter_training_data.shape[1],' variables.')
            #Giving data status
            if filter_training_data.shape[0] == 0:
                print('Your model has no data and will fail')
            elif filter_training_data.shape[0] < filter_training_data.shape[1] and filter_training_data.shape[0] > 0:
                print('You do not have enough data to run a successful model. It is recommended to loosen constraints')

            # Remove the target variables from dataframe columns 
            variable_list = column_df.features.to_list()
            variable_list.remove(target)
            variablelist = pd.DataFrame(data = [' '] + ['None'] + variable_list, columns = ['variables'])
            create_widget('08: Remove Variables?', variablelist, 'variables', 'multiselect')
            #Experimental try on alternate method
            #dbutils.widgets.dropdown('07.2: Remove Variables?',' ',variablelist.columns,'07.2: Remove Variables?')
            try:
                dbutils.widgets.remove('07.5: Error 07')
            except:
                print(' ') #printing empty so something is there to prevent error on try
            work = 8
    except:
        work = 7
        dbutils.widgets.text('07.5: Error 07', "Filter 07 failed")
else:
    print('Previous widget failed. Not running this widget')

# COMMAND ----------

# MAGIC %md
# MAGIC #10 Remove Variables allows the user to remove unwanted variables in the following data analysis. The intention is to allow the user to perform feature selection while ignoring obvious or unneeded variables

# COMMAND ----------

if work > 7:
    try:

        remvar = dbutils.widgets.get('08: Remove Variables?').split(',')

        if 'None' not in remvar:
            if ' ' in remvar:
                remvar.remove(' ')
            variable_list = [i for i in variable_list if i not in remvar]
            print('You have removed variables ',remvar , 'at', datetime.datetime.now(pytz.timezone('America/New_York')))
        else:
            print('You have selected None and removed no variables at',  datetime.datetime.now(pytz.timezone('America/New_York')))

        variables = variable_list
        dbutils.widgets.dropdown('09: Run Feature Selection?', 'No', ['Yes', 'No'])
        try:
            dbutils.widgets.remove('08.5: Error 08')
        except:
            print(' ') #printing empty so something is there to prevent error on try
        work = 9
    except:
        work = 8
        dbutils.widgets.text('08.5: Error 08', "Filter 08 failed")
else:
    print('Previous widget failed. Not running this widget')

# COMMAND ----------

# MAGIC %md
# MAGIC #11. Run Feature Selection will trigger the actual model and will take at least 2-3 minutes to complete. After it is done, a time series and a feature importance chart should appear

# COMMAND ----------

if work > 8:
    try: 
        choice = dbutils.widgets.get('09: Run Feature Selection?')
        if choice == 'No':
            print("You have choosen not run this section at ",datetime.datetime.now(pytz.timezone('America/New_York')))
        else:
            time_series_plot (filter_data,target)

            #Model selection
            selected_model = ranking_models(filter_training_data,target,variables,model_dict)

            #use dictionary to store different feature_importance functions
            important_features_function_dict = {'LinearRegression':linear_regression_feature_importance,'Ridge':ridge_regression_feature_importance,'Lasso':lasso_regression_feature_importance,'RandomForestRegressor':random_forest_feature_selection}

            # calculate the important features 
            important_features = important_features_function_dict[selected_model](filter_training_data, target, variables)
            # plot the important features
            feature_importance (important_features)

            """
            Create widget for variables selection:
            """  
        # create widge for Target Feature: 
        create_widget('10: Select Variables?', important_features, 'feature', 'multiselect')
        try:
            dbutils.widgets.remove('09.5: Error 09')
        except:
            print(' ') #printing empty so something is there to prevent error on try     
        work = 10 
                
    except: 
        work = 9
        dbutils.widgets.text('09.5: Error 09', "Filter 09 failed")
else:
    print('Previous widget failed. Not running this widget')

# COMMAND ----------

# MAGIC %md
# MAGIC #12.	 The feature importance chart ranks explanatory variables used in the model by impact from greatest to least. The longer the bar, the greater the impact. The user should focus on the variables with the longest bars and use them in the next filter

# COMMAND ----------

# MAGIC %md
# MAGIC #13.	In “10: Select Variables?”, select the explanatory variables that the user thinks have the highest chance of yielding useful insights. These should be variables are possible to change to improve the target variable results. As said before, use the results from the previous filter (the longer bars) to select the most impactful variables. The dropdown will list the variables based on their feature importance, so the best will be near the top.

# COMMAND ----------

if work > 9:
    try: 
        selected_variables = dbutils.widgets.get('10: Select Variables?').split(',')

        if selected_variables == ['']:
            print('No Variables Selected at ', datetime.datetime.now(pytz.timezone('America/New_York')))
        else:
            #Printing selected variables
            print("Selected ", selected_variables, " selected variables at ", datetime.datetime.now(pytz.timezone('America/New_York')))
            """
            If there is only one normal distribution identified, we can see the mean of normal
            IF there are more than one normal distribution identifed, we can see the splits of differnt normal distributions
            """
            for variable in selected_variables: 
                #print(variable)
                gmm_splits = get_gmm_splits(filter_training_data, variable)
                #print(gmm_splits)
                tree_splits = DTreeReg_gmm_splits (filter_training_data, target, variable)
                #print(tree_splits)
                """
                In this section, you could which splits you would like to visualize
                """
                #feature_distribution_splits(training_data,variable,gmm_splits)
                feature_distribution_splits(filter_training_data,variable,tree_splits)
                if len(tree_splits)>0:
                    BW_plot (filter_training_data,target, variable, tree_splits)
                else: 
                    print(style.RED + 'BW_plot is unavailable becasue of the constant value of selected variable')
        try:
            dbutils.widgets.remove('10.5: Error 10')
        except:
            print(' ') #printing empty so something is there to prevent error on try
        work = 11  
                
    except: 
        work = 10
        dbutils.widgets.text('10.5: Error 10', "Filter 10 failed")
else:
    print('Previous widget failed. Not running this widget')

# COMMAND ----------

# MAGIC %md
# MAGIC #14.	There will 3 outputs (histogram, chart, box and whisker plot), all of which will visualize the cut off points at which changes in your explanatory variables yield the greatest differences in the target variable. In other words, moving the explanatory variable beyond this point correlates with a large change in the target variable. It is recommended to conduct any experiments resulting from this model based on these points. An “Outlier” in the category section of the chart indicates that the data in this section is less than 10% of the total data. Use best judgement.

# COMMAND ----------

# MAGIC %md
# MAGIC # End. Ignore Below

# COMMAND ----------

# MAGIC %md
# MAGIC selected_variables = dbutils.widgets.get('07: Select Variables?').split(',')
# MAGIC
# MAGIC """
# MAGIC If there is only one normal distribution identified, we can see the mean of normal
# MAGIC IF there are more than one normal distribution identifed, we can see the splits of differnt normal distributions
# MAGIC """
# MAGIC for variable in selected_variables: 
# MAGIC     #print(variable)
# MAGIC     peaks = get_peaks(filter_training_data, variable)    
# MAGIC     tree_splits = DTreeReg_peak_splits(filter_training_data, target,variable)
# MAGIC     if len(tree_splits)>0: 
# MAGIC     #if len(peaks)>0:
# MAGIC         """
# MAGIC         In this section, you could which splits you would like to visualize
# MAGIC         """
# MAGIC         #feature_distribution_splits(training_data,variable,peaks)
# MAGIC         feature_distribution_splits(filter_training_data,variable,tree_splits)
# MAGIC     else:
# MAGIC         feature_distribution (filter_training_data,variable)
# MAGIC     
# MAGIC     if len(tree_splits)>0:
# MAGIC         BW_plot (filter_training_data,target, variable, tree_splits)
# MAGIC     else: 
# MAGIC         print(style.RED + 'BW_plot is unavailable becasue of the constant value of selected variable')
