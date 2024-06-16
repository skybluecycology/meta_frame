from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def filter_data(df, filters):
    # Apply filters to the DataFrame based on the provided filter configurations
    for f in filters:
        if f['filter_op'] == '==':
            df = df.filter(col(f['filter_col']) == f['filter_value'])
        elif f['filter_op'] == '>':
            df = df.filter(col(f['filter_col']) > f['filter_value'])
        elif f['filter_op'] == '<':
            df = df.filter(col(f['filter_col']) < f['filter_value'])
        elif f['filter_op'] == 'isin':
            df = df.filter(col(f['filter_col']).isin(f['filter_value']))
    return df

def aggregate_data(df, config):
    group_by = config['group_by']
    keep_columns = config.get('keep_columns', [])
    aggregations = config['aggregations']
    
    # Apply filters before aggregation if they exist
    if 'filters' in config:
        df = filter_data(df, config['filters'])

    # Perform aggregation
    agg_exprs = [_sum(col(item['agg_col'])).alias(item.get('new_name', item['agg_col'])) for item in aggregations]
    grouped_df = df.groupBy(group_by).agg(*agg_exprs)
    
    # Keep only the specified columns
    final_columns = group_by + keep_columns + [item.get('new_name', item['agg_col']) for item in aggregations]
    return grouped_df.select(final_columns)

def main(metadata, file_path):
    spark = SparkSession.builder.appName("AggregationEngine").getOrCreate()
    
    # Load data
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Process each iteration
    for iteration in metadata['iterations']:
        print(f"Processing {iteration['id']}...")
        for level_key, level_config in iteration.items():
            if level_key.startswith('level_'):
                print(f" - Applying {level_key}...")
                df = aggregate_data(df, level_config)
        
        # Output results of this iteration
        output_file = f"{iteration['id']}_output.csv"
        df.write.csv(output_file, header=True)
        print(f"Output saved to {output_file}")

# Example usage:
metadata = {
  "iterations": [
    {
      "id": "iteration_1",
      "level_1": {
        "group_by": ["column1", "column2"],
        "keep_columns": ["column3"],
        "aggregations": [
          {
            "agg_col": "column4",
            "agg_func": "sum",
            "new_name": "sum_column4"
          }
        ],
        "filters": [
          {
            "filter_col": "column5",
            "filter_op": ">",
            "filter_value": 100
          }
        ]
      }
      # ... other levels ...
    }
    # ... other iterations ...
  ]
}
file_path = 'path_to_your_data.csv'

if __name__ == "__main__":
    main(metadata, file_path)
  
