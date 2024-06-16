import pandas as pd
import pyspark.sql.functions as F

def load_data(data_source, data_path=None, sql_query=None):
  """Loads data based on the specified data source.

  Args:
      data_source (str): "pandas" or "spark".
      data_path (str, optional): Path to the CSV or Excel file for Pandas. Defaults to None.
      sql_query (str, optional): SQL query for Spark. Defaults to None.

  Returns:
      pandas.core.frame.DataFrame: Pandas DataFrame if data_source is "pandas".
      pyspark.sql.dataframe.DataFrame: Spark DataFrame if data_source is "spark".

  Raises:
      ValueError: If an unsupported data_source is provided.
  """

  if data_source == "pandas":
    if not data_path:
      raise ValueError("data_path is required for pandas data source")
    return pd.read_csv(data_path)
  elif data_source == "spark":
    if not sql_query:
      raise ValueError("sql_query is required for spark data source")
    return spark.sql(sql_query)
  else:
    raise ValueError(f"Unsupported data_source: {data_source}")

def aggregate_data(data_source, data_path=None, sql_query=None, meta_data):
  """Performs nested aggregation on the provided data.

  Args:
      data_source (str): "pandas" or "spark".
      data_path (str, optional): Path to the CSV or Excel file for Pandas. Defaults to None.
      sql_query (str, optional): SQL query for Spark. Defaults to None.
      meta_data (dict): JSON dictionary defining the nested aggregation logic.

  Returns:
      pandas.core.frame.DataFrame: Pandas DataFrame if data_source is "pandas".
      pyspark.sql.dataframe.DataFrame: Spark DataFrame if data_source is "spark".
  """

  df = load_data(data_source, data_path, sql_query)

  # Define nested aggregation function
  def nested_aggregate(df, meta, prefix=""):
    if isinstance(meta, dict):
      # Group and aggregate data with renaming, filtering, and selecting columns
      grouped_df = df.groupby(meta["group_by"])
      aggregation_expressions = {}
      for col, agg_def in meta["aggregate"].items():
        if isinstance(agg_def, str):
          aggregation_expressions[col] = F.col(col).alias(f"{prefix}{meta['rename'][col]}")
        else:
          filter_cond = F.col(col) if col not in meta.get("filter", []) else meta["filter"][col]
          aggregation_expressions[col] = (
              filter_cond.filter(agg_def.get("filter", lambda x: True))
              .agg(agg_def["function"])
              .alias(f"{prefix}{meta['rename'][col]}")
          )
      aggregated_df = grouped_df.agg(**aggregation_expressions)
      # Join with child tables and perform nested aggregation
      for child in meta.get("children", []):
        joined_df = aggregated_df.join(df.select(*child["link"]), how="left")
        aggregated_df = nested_aggregate(joined_df, child, f"{prefix}{meta['id']}_")
      return aggregated_df
  else:
      # Base case: return original dataframe for atomic elements
      return df.withColumn("id", F.lit(meta["id"]))

  # Apply nested aggregation on loaded data
  result_df = nested_aggregate(df.copy(), meta_data)

  # Ensure bitemporality (replace with your logic)
  result_df = result_df.withColumn("valid_from", F.lit("2024-01-01"))
  result_df = result_df.withColumn("valid_to", F.lit("9999-12-31"))

  return result_df

# Example usage:

meta_data = "{
  "group_by": ["col1", "col2"],
  "aggregate": {
    "col3": "sum",
    "col4": {
      "filter": "col4 > 10",
      "function": "avg"
    }
  },
  "rename": {
    "col3": "total_col3",
    "col4": "avg_col4_gt_10"
  },
  "filter": ["col1 != 'excluded_value'"],  // Optional filtering at top level
  "children": [
    {
      "id": "child_1",
      "link": ["col5"],  // Columns to join with parent table
      "aggregate": {"col6": "count"},
      "rename": {"col6": "count_col6"}
    }
  ]
}
"

# Load data from CSV using pandas
pandas_df = aggregate_data("pandas", data_path="data.csv", meta_data=meta_data)

# Load data from Databricks SQL query using Spark
spark_df = aggregate_data("spark", sql_query="SELECT * FROM my_table", meta_data=meta_data)

# Display or write results to desired format
pandas_df.show()  # For pandas DataFrame
spark_df.show()  # For Spark DataFrame

      
