from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max, min as _min
import pandas as pd

def aggregate_and_join(df, config, parent_id=None):
    # Check if we should use pandas or Spark
    use_pandas = config.get('use_pandas', False)
    
    if use_pandas:
        # Convert Spark DataFrame to pandas DataFrame if needed
        df = df.toPandas()
    
    # Base case: if df is empty or no more children, save if lowest level
    if (use_pandas and df.empty) or (not use_pandas and df.rdd.isEmpty()) or ('children' not in config and parent_id is not None):
        # Save logic here using unique_id from config if 'save' is True
        if config.get('save', False):
            if use_pandas:
                df.to_csv(f"{config['unique_id']}.csv", index=False)
            else:
                df.write.csv(f"{config['unique_id']}.csv")
        return df
    
    # Apply configurable filtering
    if 'filters' in config:
        for filter_condition in config['filters']:
            if use_pandas:
                df = df.query(filter_condition)
            else:
                df = df.filter(filter_condition)
    
    # Keep specified columns
    if 'keep_columns' in config:
        if use_pandas:
            df = df[config['keep_columns']]
        else:
            df = df.select(config['keep_columns'])
    
    # Perform aggregation based on configuration
    if 'aggregation' in config:
        agg_exprs = {}
        for col_name, agg_funcs in config['aggregation']['aggregations'].items():
            for agg_func in agg_funcs:
                if use_pandas:
                    agg_exprs[col_name] = agg_func
                else:
                    if agg_func == 'sum':
                        agg_exprs[f'sum_{col_name}'] = _sum(col_name)
                    elif agg_func == 'max':
                        agg_exprs[f'max_{col_name}'] = _max(col_name)
                    elif agg_func == 'min':
                        agg_exprs[f'min_{col_name}'] = _min(col_name)
        
        if use_pandas:
            df = df.groupby(config['aggregation']['group_by']).agg(agg_exprs).reset_index()
        else:
            df = df.groupBy(config['aggregation']['group_by']).agg(*agg_exprs.values())
        
        # Rename aggregated columns if specified
        for old_name, new_name in config['aggregation'].get('rename', {}).items():
            if use_pandas:
                df.rename(columns={old_name: new_name}, inplace=True)
            else:
                df = df.withColumnRenamed(old_name, new_name)
    
    # If there are children, recursively call function on child dataframes
    if 'children' in config:
        for child_config in config['children']:
            child_df = pd.read_csv(child_config['data_path']) if use_pandas else spark.read.csv(child_config['data_path'], header=True)
            child_df = aggregate_and_join(child_df, child_config, parent_id=config['unique_id'])
            
            # Join with parent dataframe based on configuration
            if 'join' in child_config:
                if use_pandas:
                    df = pd.merge(df, child_df, left_on=child_config['join']['on'], right_on=child_config['join']['on'], how=child_config['join']['how'])
                else:
                    df = df.join(child_df, col(child_config['join']['on']), child_config['join']['how'])
    
    return df

# Example usage:
config = {
    'use_pandas': True,  # Set this to False to use Spark instead
    'unique_id': 'parent',
    'save': True,
    'filters': [
        "column_name > value",
        "another_column < another_value"
        # ... more filter conditions ...
    ],
    'keep_columns': ['column1', 'column2'],
    'aggregation': {
        'group_by': ['column1'],
        'aggregations': {
            'column2': ['sum', 'max', 'min'],
            # ... other columns with desired aggregations ...
        },
        'rename': {
            'sum(column2)': 'total_column2',
            'max(column2)': 'max_column2',
            'min(column2)': 'min_column2'
            # ... other