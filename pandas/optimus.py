'''
Write a Meta driven aggregation and join function with python pandas. It should behave with recursive child parent levels also one parent can have many child processes. Processes should be able to read configuration for aggregation, keep columns, filtering and join. There should also be a list of parents with an id. The lowest level child process should have a save option and unique id that is used when saving the result dataframe.
'''

import pandas as pd

def aggregate_and_join(df, config, parent_id=None):
    # Base case: if df is empty or no more children, save if lowest level
    if df.empty or ('children' not in config and parent_id is not None):
        # Save logic here using unique_id from config if 'save' is True
        if config.get('save', False):
            df.to_csv(f"{config['unique_id']}.csv")
        return df
    
    # Apply configurable filtering
    if 'filters' in config:
        for filter_condition in config['filters']:
            df = df.query(filter_condition)
    
    # Keep specified columns
    if 'keep_columns' in config:
        df = df[config['keep_columns']]
    
    # Perform aggregation based on configuration
    if 'aggregation' in config:
        df = df.groupby(config['aggregation']['group_by']).agg(config['aggregation']['aggregations'])
        # Rename aggregated columns if specified
        if 'rename' in config['aggregation']:
            df.rename(columns=config['aggregation']['rename'], inplace=True)
    
    # If there are children, recursively call function on child dataframes
    if 'children' in config:
        for child_config in config['children']:
            child_df = pd.read_csv(child_config['data_path'])  # Assuming CSV format for simplicity
            child_df = aggregate_and_join(child_df, child_config, parent_id=config['unique_id'])
            # Join with parent dataframe based on configuration
            if 'join' in child_config:
                df = pd.merge(df, child_df, on=child_config['join']['on'], how=child_config['join']['how'])
    
    return df

# Example usage:
config = {
    'unique_id': 'parent',
    'save': True,
    'filters': [
        'column_name > value',
        'another_column < another_value'
        # ... more filter conditions ...
    ],
    'keep_columns': ['column1', 'column2'],
    'aggregation': {
        'group_by': ['column1'],
        'aggregations': {'column2': 'sum'},
        'rename': {'column2_sum': 'aggregated_column2'}
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

# Assuming you have a dataframe `df` loaded from your parent data source.
result_df = aggregate_and_join(df, config)
