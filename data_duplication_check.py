from typing import (    
    List,
    Optional
)
import logging
import copy

import pandas as pd

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class DuplicationCheck:
    
    def __init__(
        self,
        df: pd.DataFrame,
        single_cols_check: Optional[List[str]] = None,
        multi_cols_dup_check: Optional[List[str]] = None,
    ) -> None:
    
        self.df = df
        self.single_cols_check = single_cols_check
        self.multi_cols_dup_check = multi_cols_dup_check
        
        self.dup_cols_list = None
        self.df_dedup = None
        self.same_data_cols = None
     

    @staticmethod
    def get_dup_col_indexes(df: pd.DataFrame,col_name: str) -> List:
    
        """
        This function is used to get the location/index of duplicated columns
        If the given column is note duplicated in the dataset, the  function
        will not work

        params
        ------

        df : dataset
        col_name : name of the column that is duplicated

        returns
        -------
        Indexes of the duplicated columns in a list

        """

        if col_name not in list(df.columns[df.columns.duplicated()]):
            raise KeyError("The input column is not duplicated. to get the index just use get_loc(col_name) ")

        indexes = [i for i,j in enumerate(df.columns.get_loc(col_name)) if j==True]

        return indexes
    

    def check_column_name_duplication(self):
        
        
        self.df_dedup = pd.DataFrame(columns = copy.deepcopy(self.df.columns), data = copy.deepcopy(self.df.values))
        
        self.dup_cols_list = list(set((self.df_dedup.columns[self.df_dedup.columns.duplicated()])))
       
        dup_names_diff_vals = []
        dup_names_dup_vals = []    
        
        for x in self.dup_cols_list:
          
            duped_cols_indexes = self.get_dup_col_indexes(self.df_dedup,x)
            
            for i,j in enumerate(duped_cols_indexes[1:]):
                self.df_dedup.columns.values[j] = x+'_dup_'+str(i)
                         
            dup_cols = [col for col in self.df_dedup.columns if x in col]         
            
            for i in range(1,len(dup_cols)):
                equal_flag = self.df_dedup[dup_cols[0]].equals(self.df_dedup[dup_cols[i]])

                if equal_flag:
                    self.df_dedup = self.df_dedup.drop(columns=[dup_cols[i]])
                    dup_names_dup_vals.append(x+': '+dup_cols[i])
                else:
                    dup_names_diff_vals.append(x+': '+dup_cols[i])

        logging.info(f'duplicated columns removed : {dup_names_dup_vals},duplicated column names but different values, These were not removed, need to check manually: {dup_names_diff_vals}')

    def check_same_data_duplication(self):
        
        if self.df_dedup is not None: 
            
            all_cols = list(self.df_dedup)
        
            self.same_data_cols = []

            for i in all_cols:
                for j in all_cols:
                    if i != j:
                        equal_flag = self.df_dedup[i].equals(self.df_dedup[j])

                        if equal_flag:
                            if j+' '+i not in self.same_data_cols:
                                self.same_data_cols.append(i+' '+j) 
        
            for i in self.same_data_cols:
                remove_col_name = i.split(' ')[1] #remove the second column with same data
                self.df_dedup = self.df_dedup.drop(columns=[remove_col_name])
                logging.info(f"column {remove_col_name} removed becasue it has same data as { i.split(' ')[0]}")

                return self.df_dedup
        
        else:
            raise AttributeError("Need to run 'check_column_name_duplication' function first")
               
        
    def get_duplicated_col_name(self):
        
        if self.dup_cols_list is not None:      
            return  self.dup_cols_list
        else:
            raise AttributeError("Need to run 'check_column_name_duplication' function first")

            
    def get_dedup_df(self):
        
        if self.df_dedup is not None:    
            return  self.df_dedup
        else:
            raise AttributeError("Need to run 'check_column_name_duplication' function first")
            
    def get_duplicated_data_col_names(self):
        
        if self.same_data_cols is not None:      
            return  self.same_data_cols
        else:
            raise AttributeError("Need to run 'check_same_data_duplication' function first")
            
            
    def check_row_duplication(self):
        
        if self.df_dedup is not None:           
            # on all columns
            self.df_dedup_row = self.df_dedup.drop_duplicates(keep='first') 
            all_n_rows = len(self.df_dedup)
            all_n_rows_deduped = len(self.df_dedup_row)
            
            if all_n_rows > all_n_rows_deduped:
                logging.info(f"{all_n_rows-all_n_rows_deduped} rows removed due to row duplication based on all columns")
            else:
                 logging.info("No row duplication on all columns")
                    
            if self.single_cols_check is not None:
                
                all_n_rows = len(self.df_dedup_row)
                
                for i in self.single_cols_check:
                    self.df_dedup_row = self.df_dedup_row.drop_duplicates(subset=[i],keep='first') 
                
                all_n_rows_deduped = len(self.df_dedup_row)
                
                if all_n_rows > all_n_rows_deduped:
                    logging.info(f"{all_n_rows-all_n_rows_deduped} rows removed due to row duplication based specified single columns")
                else:
                     logging.info("No row duplication based on specified columns")
                        
            if self.multi_cols_dup_check is not None:
                
                all_n_rows = len(self.df_dedup_row)          
               
                self.df_dedup_row = self.df_dedup_row.drop_duplicates(subset=self.multi_cols_dup_check,keep='first') 
                
                all_n_rows_deduped = len(self.df_dedup_row)
                
                if all_n_rows > all_n_rows_deduped:
                    logging.info(f"{all_n_rows-all_n_rows_deduped} rows removed due to row duplication based specified column group")
                else:
                     logging.info("No row duplication based on specified column group") 
                    
            return self.df_dedup_row
                
        else:
            raise AttributeError("Need to run 'check_column_name_duplication' function first")