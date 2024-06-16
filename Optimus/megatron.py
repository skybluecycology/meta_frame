import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max, min as _min

def filter_data(df, filters, use_spark):
    # Apply filters to the DataFrame based on the provided filter configurations
    for f in filters:
        if use_spark:
            # Spark DataFrame filtering
            df = df.filter(col(f['filter_col']).eqNullSafe(f['filter_value']) if f['filter_op'] == '==' else col(f['filter_col']) > f['filter_value'] if f['filter_op'] == '>' else col(f['filter_col']) < f['filter_value'] if f['filter_op'] == '<' else col(f['filter_col']).isin(f['filter_value']) if f['filter_op'] == 'isin' else df)
        else:
            # Pandas DataFrame filtering
            df = df[df[f['filter_col']] == f['filter_value']] if f['filter_op'] == '==' else df[df[f['filter_col']] > f['filter_value']] if f['filter_op'] == '>' else df[df[f['filter_col']] < f['filter_value']] if f['filter_op'] == '<' else df[df[f['filter_col']].isin(f['filter_value'])] if f['filter_op'] == 'isin' else df
    return df

def aggregate_data(df, config, use_spark):
    group_by = config['group_by']
    keep_columns = config.get('keep_columns', [])
    aggregations = config['aggregations']
    
    # Apply filters before aggregation if they exist
    if 'filters' in config:
        df = filter_data(df, config['filters'], use_spark)

    # Perform aggregation
    if use_spark:
        # Spark aggregations
        agg_exprs = []
        for item in aggregations:
            if item['agg_func'] == 'sum':
                agg_exprs.append(_sum(col(item['agg_col'])).alias(item.get('new_name', item['agg_col'])))
            elif item['agg_func'] == 'max':
                agg_exprs.append(_max(col(item['agg_col'])).alias(item.get('new_name', item['agg_col'])))
            elif item['agg_func'] == 'min':
                agg_exprs.append(_min(col(item['agg_col'])).alias(item.get('new_name', item['agg_col'])))
        grouped_df = df.groupBy(group_by).agg(*agg_exprs)
        final_columns = group_by + keep_columns + [item.get('new_name', item['agg_col']) for item in aggregations]
        return grouped_df.select(final_columns)
    else:
        # Pandas aggregations
        agg_dict = {item['agg_col']: item.get('new_name', item['agg_func']) for item in aggregations}
        grouped_df = df.groupby(group_by).agg(agg_dict)
        
        # Flatten MultiIndex if created during aggregation
        if isinstance(grouped_df.columns, pd.MultiIndex):
            grouped_df.columns = [' '.join(col).strip() for col in grouped_df.columns.values]
        
        # Reset index to turn group by columns into cols
        grouped_df.reset_index(inplace=True)
        
        # Keep only the specified columns
        final_columns = group_by + keep_columns + [item.get('new_name', item.get('new_name', item['agg_func'])) for item in aggregations]
        return grouped_df[final_columns]

# ... (previous code)

def main(metadata, file_path, use_spark=False):
    if use_spark:
        spark = SparkSession.builder.appName("AggregationEngine").getOrCreate()
        # Load data into a Spark DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    else:
        # Load data into a pandas DataFrame
        df = pd.read_csv(file_path)
    
    # Process each iteration defined in metadata
    for iteration in metadata['iterations']:
        print(f"Processing {iteration['id']}...")
        for level_key, level_config in iteration.items():
            if level_key.startswith('level_'):
                print(f" - Applying {level_key}...")
                df = aggregate_data(df, level_config, use_spark)
        
        # Output results of this iteration
        output_file = f"{iteration['id']}_output.csv"
        
        if use_spark:
            df.write.csv(output_file, header=True)
        else:
            df.to_csv(output_file, index=False)
        
        print(f"Output saved to {output_file}")

# Example JSON configuration
example_metadata = {
    "iterations": [
        {
            "id": "iteration_1",
            "level_1": {
                "group_by": ["column1", "column2"],
                "keep_columns": ["column3"],
                "aggregations": [
                    {"agg_col": "column4", "agg_func": "sum", "new_name": "total_column4"},
                    {"agg_col": "column5", "agg_func": "max"},
                    {"agg_col": "column6", "agg_func": "min"}
                ]
            }
        }
    ]
}

# Save example configuration to a JSON file
with open('example_metadata.json', 'w') as json_file:
    json.dump(example_metadata, json_file, indent=4)

# To run the main function with the example metadata and a CSV file path:
# main(example_metadata, 'path_to_your_csv.csv', use_spark=False)
