from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max, min as _min

def aggregate_and_join(df, config, parent_id=None):
    spark = SparkSession.builder.getOrCreate()
    
    # Base case: if df is empty or no more children, save if lowest level
    if df.rdd.isEmpty() or ('children' not in config and parent_id is not None):
        # Save logic here using unique_id from config if 'save' is True
        if config.get('save', False):
            df.write.csv(f"{config['unique_id']}.csv")
        return df
    
    # Apply configurable filtering
    if 'filters' in config:
        for filter_condition in config['filters']:
            df = df.filter(filter_condition)
    
    # Keep specified columns
    if 'keep_columns' in config:
        df = df.select(config['keep_columns'])
    
    # Perform aggregation based on configuration
    if 'aggregation' in config:
        agg_exprs = {}
        for col_name, agg_funcs in config['aggregation']['aggregations'].items():
            for agg_func in agg_funcs:
                if agg_func == 'sum':
                    agg_exprs[f'sum_{col_name}'] = _sum(col_name)
                elif agg_func == 'max':
                    agg_exprs[f'max_{col_name}'] = _max(col_name)
                elif agg_func == 'min':
                    agg_exprs[f'min_{col_name}'] = _min(col_name)
        
        df = df.groupBy(config['aggregation']['group_by']).agg(*agg_exprs.values())
        
        # Rename aggregated columns if specified
        for old_name, new_name in config['aggregation'].get('rename', {}).items():
            df = df.withColumnRenamed(old_name, new_name)
    
    # If there are children, recursively call function on child dataframes
    if 'children' in config:
        for child_config in config['children']:
            child_df = spark.read.csv(child_config['data_path'], header=True)  # Assuming CSV format with header
            child_df = aggregate_and_join(child_df, child_config, parent_id=config['unique_id'])
            # Join with parent dataframe based on configuration
            if 'join' in child_config:
                df = df.join(child_df, col(child_config['join']['on']), child_config['join']['how'])
    
    return df

# Example usage:
config = {
    'unique_id': 'parent',
    'save': True,
    'filters': [
        col('column_name') > value,
        col('another_column') < another_value
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
            # ... other renamed aggregated columns ...
        }
    },
    # ... rest of configuration including children ...
}

# Assuming you have a Spark DataFrame `df` loaded from your parent data source.
result_df = aggregate_and_join(df, config)

'''
config = {
    'unique_id': 'parent',
    'save': True,
    'filters': [
        col('column_name') > value,
        col('another_column') < another_value
        # ... more filter conditions ...
    ],
    'keep_columns': ['column1', 'column2'],
    'aggregation': {
        'group_by': ['column1'],
        'aggregations': {'column2': 'sum'},
        'rename': {'sum(column2)': 'aggregated_column2'}
    },
    'children': [
        {
            'unique_id': 'child',
            'data_path': 'child_data.csv',
            'join': {'on': 'common_column', 'how': 'inner'},
            # Child's own configuration can be added here...
            # This can include its own children as well, making it recursive.
        }
        # More children configurations can be added here...
    ]
}
'''