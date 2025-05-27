from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.types import StructType, ArrayType

from pyspark.sql.functions import col, explode_outer, lit

 

class Flattener:

    """

    A class for processing and transforming nested DataFrames in PySpark, including

    flattening of nested structures and dynamic handling of complex schemas.

 

    Attributes:

        spark (SparkSession): Spark session object.

        logger (CustomLogger, optional): Logger instance to log messages. Defaults to None.

    """

 

    def __init__(self, spark: SparkSession, flattening_type: str, logger=None):

        """

        Initializes the DataFrameProcessor with the provided Spark session and logger.

 

        Args:

            spark (SparkSession): Spark session object to use for processing DataFrames.

            logger (CustomLogger, optional): Logger instance to log messages. Defaults to None.

        """

        self.spark = spark

        self.flattening_type = flattening_type

        self.logger = logger

 

    def find_field_types(self, df: DataFrame, subfield_name: str = '') -> tuple:

        """

        Identifies and categorizes fields in a DataFrame as either simple fields,

        structured fields with a specific subfield, or structured fields without the subfield.

 

        Args:

            df (DataFrame): The DataFrame to analyze.

            subfield_name (str): The name of the subfield to look for in structured fields (optional).

 

        Returns:

            tuple: Three lists containing simple fields, structured fields with the specified subfield,

                   and structured fields without the subfield, respectively.

        """

        if self.logger:

            self.logger.log_message("Finding field types in the DataFrame...", level="info")

        struct_fields_with_subfield = []

        struct_fields_without_subfield = []

        simple_fields = []

 

        for field in df.schema.fields:

            if isinstance(field.dataType, StructType):

                if subfield_name in [subfield.name for subfield in field.dataType.fields]:

                    struct_fields_with_subfield.append(field.name)

                else:

                    struct_fields_without_subfield.append(field.name)

            elif not isinstance(field.dataType, ArrayType):

                simple_fields.append(field.name)

 

        return list(set(simple_fields)), list(set(struct_fields_with_subfield)), list(set(struct_fields_without_subfield))

 

    def aliasing(self, fields_list: list, value_field: str = "value") -> list:

        """

        Generates alias names for fields in the DataFrame to simplify nested structures.

 

        Args:

            fields_list (list): A list of fields to alias.

            value_field (str): The specific subfield name (default is 'value').

 

        Returns:

            list: A list of aliased column expressions.

        """

        if self.logger:

            self.logger.log_message("Aliasing fields...", level="info")

        fields_list_alias = []

 

        for field_name in fields_list:

            if "." in field_name:

                if field_name.split(".")[1] == value_field:

                    new_col = col(field_name).alias(field_name.split('.')[0])

                else:

                    new_col = col(field_name).alias(field_name.split('.')[1])

            else:

                new_col = field_name

 

            fields_list_alias.append(new_col)

        # if self.logger:

        #     self.logger.log_message(f"Aliased fields: {fields_list_alias}", level="info")

        return fields_list_alias

 

    def reform_dataframe(self, df_standard: DataFrame, drop_fields: list) -> DataFrame:

        """

        Reforms the DataFrame based on the specified flattening type by selecting or dropping fields.

 

        Args:

            df_standard (DataFrame): The standard DataFrame to reform.

            drop_fields (list): A list of fields to drop from the DataFrame.

 

        Returns:

            DataFrame: The reformed DataFrame after applying the specified transformations.

 

        Raises:

            ValueError: If the flattening type is not defined.

        """

        if self.logger:

            self.logger.log_message("Starting to reform DataFrame...", level="info")

        try:

            if self.flattening_type == "table_list":

                df = df_standard.select("*", 'Table_List.*').drop(*drop_fields)

                if self.logger:

                    self.logger.log_message("Reformed DataFrame using 'table_list' flattening type.", level="info")

            elif self.flattening_type == "nested":

                df = df_standard.drop(*drop_fields)

                if self.logger:

                    self.logger.log_message("Reformed DataFrame using 'nested' flattening type.", level="info")

            else:

                raise ValueError("Flattening Type not Defined")

        except Exception as e:

            err_msg = f"Error during DataFrame reformation: {e}"

            if self.logger:

                self.logger.log_message(err_msg, level="error")

                self.logger.shutdown_log()

            raise ValueError(err_msg)

        if self.logger:

            self.logger.log_message("DataFrame reformation completed successfully.", level="info")

        return df

 

    def dynamic_flatten(self, df: DataFrame, parent_fields: list = [], nested_tables: str = '', audit_fields: list = [], main_field: str = 'id', prev_table: str = None) -> DataFrame:

        """

        Dynamically flattens nested structures in the DataFrame by exploding nested fields

        and processing each level of the hierarchy.

 

        Args:

            df (DataFrame): The DataFrame to flatten.

            parent_fields (list): A list of parent fields to retain in the flattened structure.

            nested_tables (str): A dot-separated string representing the nested field hierarchy.

            audit_fields (list): A list of fields to include for auditing purposes.

            main_field (str): The primary field to use as an identifier (default is 'id').

            prev_table (str): The name of the previous table in the hierarchy (optional).

 

        Returns:

            DataFrame: The flattened DataFrame after processing the nested fields.

        """

        try:

            if self.logger:

                self.logger.log_message("Starting dynamic flattening process...", level="info")

 

            nested_table_list = nested_tables.split(".")

            nested_table = nested_table_list.pop(0)

 

            if not nested_table:

                if self.logger:

                    self.logger.log_message("No nested table provided, returning the original DataFrame.", level="warning")

                return df

 

            id_field = f"{prev_table}_{main_field}" if prev_table else main_field

            parent_fields_audit = list(set(parent_fields + audit_fields))

            parent_fields = self.aliasing(parent_fields_audit)

            if self.logger:

                self.logger.log_message(f"Exploding nested table '{nested_table}'", level="info")

            try:

                exploded_df = (df.withColumnRenamed(main_field, f"{id_field}")

                                .select(parent_fields + [nested_table])

                                .withColumn(nested_table, explode_outer(col(nested_table)))

                                .filter(col(nested_table).isNotNull())

                            )

 

                simple_fields, struct_fields_with_subfield, struct_fields_without_subfield = self.find_field_types(exploded_df)

                selected_columns = audit_fields + list(set(simple_fields).difference(set(audit_fields))) + [col(nested_table + ".*")]

                if self.logger:

                    self.logger.log_message("Selecting columns from exploded DataFrame", level="info")

                new_df = exploded_df.select(*selected_columns)

 

            except Exception as e:

                if self.logger:

                    self.logger.log_message(f"Error during dynamic flatten: {e}", level="error")

                return exploded_df.filter(lit(1) == lit(2))  # Return empty DataFrame

 

            parent_fields = simple_fields + [f"{nested_table}_{main_field}"]

            if self.logger:

                self.logger.log_message(f"Flattening completed for nested table '{nested_table}'. Moving to the next level if present.", level="info")

 

            return self.dynamic_flatten(new_df, parent_fields, '.'.join(nested_table_list), audit_fields, main_field, nested_table)

        except Exception as e:

            err_msg = f"Error during Dynamic Flattening: {e}"

            if self.logger:

                self.logger.log_message(err_msg, level="error")

                self.logger.shutdown_log()

            raise ValueError(err_msg)

 

    def drop_immediate_childs(self, df: DataFrame, table_name: str, all_tables: list) -> DataFrame:

        """

        Drops immediate child columns from the DataFrame based on the provided table name and list of all tables.

 

        Args:

            df (DataFrame): The Spark DataFrame from which columns will be dropped.

            table_name (str): The name of the table to identify child columns.

            all_tables (List[str]): A list of all tables with array fields.

 

        Returns:

        DataFrame: The DataFrame with the immediate child columns dropped.

 

        Raises:

            Error on any Failure with message

        """

        try:

            all_child_tables = [t.replace(table_name, "") for t in all_tables if table_name in t]

            immediate_child_tables = [ac.replace(".", "") for ac in all_child_tables if len(ac.split('.')) == 2]

 

            if self.logger:

                self.logger.log_message(f"Immediate child columns identified: {immediate_child_tables}", "info")

 

            if len(immediate_child_tables) != 0:

                df = df.drop(*immediate_child_tables)

 

            return df

        except Exception as e:

            err_msg = f"Error in dropping immediate child columns: {str(e)}"

            if self.logger:

                self.logger.log_message(err_msg, "error")

                self.logger.shutdown_log()

            raise ValueError(err_msg)