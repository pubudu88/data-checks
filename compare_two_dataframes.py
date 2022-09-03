from typing import (    
    List,
    Optional
)
import logging

import pandas as pd
import numpy as np

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class CompareDataframes:
    
    def __init__(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        float_cols: Optional[List[str]] = None,
        rounding_places: Optional[int] = None
    ) -> None:

        """
        params
        ------
        df1 : dataframe 1
        df2 : dataframe 2
        float_cols : columns which has data with many decimal points
        rounding_places : when comparing float columns need to round them and then compare. Set the rounding places.

        """
            
        self.df1 = df1.copy()
        self.df2 = df2.copy()
        self.float_cols = float_cols
        self.rounding_places = rounding_places
        
        self.indexes_different = True
        self.common_cols = None
        self.cols_missing_in_df1 = None
        self.cols_missing_in_df2 = None
        
        if float_cols is not None and rounding_places is None:
            raise  ValueError('If foat cols is specified, need to specify rounding places as well') 
            
        if float_cols is  None and rounding_places is not None:
            raise  ValueError('If rounding places is specified, need to specify float cols as well') 
        
        if self.df1.equals(self.df2):
            logging.info('The two dataframes are equal')
        else:
            logging.info('The two dataframes are different, more analysis needed to indentify the difference')
            logging.info(f'The shape of dataframe 1 is {self.df1.shape} and the shape of the dataframe 2 is {self.df2.shape}')
            
            
    def compare_indexes(self) -> None:
        
        """
        Check if the indexes of the two dataframes are identical. The order needs
        to the same as well
        
        """
        
        if np.array_equal(self.df1.index,self.df2.index):
            logging.info('The indexes of the two dataframes are the same') 
            self.indexes_different =  False
        else:
            logging.info('The indexes of the two dataframes are different')
        
    
    def compare_column_names(self) -> None:
        
        """
       Compare the columns of both dataframes and put following items in 
       variables
       - common columns in both dataframes : common_cols
       - columns present in df1 but missing in df2 : cols_missing_in_df2
       - columns present in df2 but missing in df1 : cols_missing_in_df1
        
        """
        
        if set(list(self.df1)) == set(list(self.df2)):
            logging.info('The columns are identical in the two dataframes')
            self.common_cols = list(self.df1)
            self.cols_missing_in_df1 = []
            self.cols_missing_in_df2 = []
            
            if self.df1[self.common_cols].equals(self.df2[self.common_cols]):
                logging.info('The two dataframes are equal when columns are ordered')
            else:
                logging.info('The two dataframes are different even when the columns are ordered to be the same\
                             , more analysis needed to indentify the difference')
            
        else:
            logging.info('The columns are different in the two dataframes')
            self.common_cols = [x for x in list(self.df1) if x in list(self.df2)]
            self.cols_missing_in_df1 = [x for x in list(self.df2) if x not in list(self.df1)]
            self.cols_missing_in_df2 = [x for x in list(self.df1) if x not in list(self.df2)]
            
            
    def get_missing_cols_in_df1(self) -> List:
        
        """
        returns columns that are present in df2 but missing in df1
        """
        if self.cols_missing_in_df1 is None:
            raise AttributeError("Need to run 'compare_column_names' function first")    
        else:
            return self.cols_missing_in_df1
    
    
    def get_missing_cols_in_df2(self) -> List:
        
        """
        returns columns that are present in df1 but missing in df2
        """
        
        if self.cols_missing_in_df2 is None:
            raise AttributeError("Need to run 'compare_column_names' function first")    
        else:
            return self.cols_missing_in_df2
    
    
    def get_common_cols_in_df1_df2(self) -> List:
        
        """
        returns columns that are present in both df1 and df2
        """
        
        if self.common_cols is None:
            raise AttributeError("Need to run 'compare_column_names' function first")           
        else:
            return self.common_cols
        
        
    def set_common_cols(self,common_columns: List[str]) -> None:
        
        """
        If required manually set the common columns in the two
        dataframes, if this set, 'get_common_cols_in_df1_df2' function is not
        required to run
        """
        self.common_cols = common_columns
    
    
    def compare_common_cols_values(self) -> pd.DataFrame:
        
        """
        For common columns check which columns are different in values. For this function to work
        indexes of the dataframes and the shapes should be identical
        
        This function returns a dataframe with column names and showing the proportion of similar
        rows for each column, and if the similar proprotion is less than 1, it will show the indexes
        where the difference are
        """
        
        self.similar_props = []
        self.diff_indexes = []
        
        if self.indexes_different:
            raise IndexError("Indexes of the dataframes should match to continue the analysis, make sure compare_indexes function is run first")           
        
        if self.df1.shape != self.df2.shape:
            raise AttributeError("shapes of the dataframes should match to continue the analysis")
        
        if self.float_cols is not None:
            self.invalid_float_cols = [x for x in self.float_cols if x not in self.common_cols]

            if len(self.invalid_float_cols)>0:
                logging.info(f'Invalid columns in float cols: {self.invalid_float_cols}')
                raise KeyError('Some columns specified in float cols are not in common cols')                
            else:
                for i in self.float_cols:
                    self.df1[i] = self.df1[i].astype(float)
                    self.df1[i] = self.df1[i].round(self.rounding_places)
                    
                    self.df2[i] = self.df2[i].astype(float)
                    self.df2[i] = self.df2[i].round(self.rounding_places)
                    
        for i in self.common_cols:
            
            if self.df1[[i]].equals(self.df2[[i]]):
                similar_prop = 1
                diff_indexes = None
            else:
                similar_prop = (self.df1[i] == self.df2[i]).sum()/len(self.df1)
                
                s = self.df1[i] == self.df2[i]
                diff_indexes = s[s==False]
                
            self.similar_props.append(similar_prop)
            self.diff_indexes.append(diff_indexes)
            
            
        self.df_similar_props = pd.DataFrame({'column':self.common_cols
                                              ,'similar_prop':self.similar_props
                                              ,'different_indexes':self.diff_indexes}).set_index('column')
        
        return self.df_similar_props
    
    
    def get_different_values_df1(self,diff_col_name:str) -> pd.DataFrame:
        
        """
        For a given column is df1 show which values are different from df2
        """
        
        return self.df1.iloc[list(self.df_similar_props.loc[diff_col_name
                                                            ,'different_indexes'].index),:]
            
    
    def get_different_values_df2(self,diff_col_name:str) -> pd.DataFrame:
        
        """
        For a given column is df2 show which values are different from df1
        """
        
        return self.df2.iloc[list(self.df_similar_props.loc[diff_col_name
                                                            ,'different_indexes'].index),:]