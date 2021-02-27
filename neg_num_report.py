from .data_elements_query import DataQuery,data_main,data_meta



def schema():

    query_data = DataQuery(data_main, data_meta)
    gen_format_items = query_data.format_generation()

    data = {
        "name": "Report",
        "properties":{
                "host_name": query_data.host_name,
                "Data Store Name": "",
                "Database Name": query_data.db_name,
                "Table Name": query_data.table_name
            },

        "type": "record",

        "fields": [
            {
                "name": "Column",
                "type": "string",
                "values": "",
            },
            {
                "name": "inferred_data_class",
                "type": "string",
                "values": "",
            },

#metadata
            {
                "name": "MetaData",
                "type": {
                    "name": "MetaData",
                    "type": "record",
                    "fields": [
                        {
                            "name": "Inferred",
                            "type": {
                                "name": "Inferred",
                                "type": "record",
                                "fields": [
                                    {
                                        "name": "Data_type",
                                        "type": "string",
                                        "values": "",
                                    },
                                    {
                                        "name": "data_length",
                                        "type": "int",
                                        "values": "",
                                    },
                                    {
                                        "name": "Precision",
                                        "type": "int",
                                        "values": query_data.num_prec,
                                    },
                                    {
                                        "name": "scale",
                                        "type": "int",
                                        "values": "",
                                    },
                                    {
                                        "name": "data_scale",
                                        "type": "int",
                                        "values": "",
                                    }
                                ]
                            }
                        },
                        {
                            "name": "Defined",
                            "type": {
                                "name": "Defined",
                                "type": "record",
                                "fields": [
                                    {
                                        "name": "Data_type",
                                        "type": "string",
                                        "values": "",
                                    },
                                    {
                                        "name": "data_lenght",
                                        "type": "int"
                                    },
                                    {
                                        "name": "Precision",
                                        "type": "int"
                                    },
                                    {
                                        "name": "scale",
                                        "type": "int"
                                    },
                                    {
                                        "name": "data_scale",
                                        "type": "int"
                                    }
                                ]
                            }
                        },
                        {
                            "name": "Defined",
                            "type": {
                                "name": "Defined",
                                "type": "record"
                            }
                        }
                    ]
                }
            },
# Domain Higlights
            {
                "name": "domain_highlights",
                "type": {
                    "name": "domain_highlights",
                    "type": "record",
                    "fields": [
                        {
                            "name": "TotalRows",
                            "type": "int",
                            "values": query_data.total_rows,
                        },
                        {
                            "name": "Cardinality",
                            "type": "int",
                            "values": query_data.cardinality,
                        },
                        {
                            "name": "per_of_Cardinality",
                            "type": "int",
                            "values": query_data.perc_of_cardinality,
                        },
                        {
                            "name": "Nulls",
                            "type": "int",
                            "values": query_data.num_null_values,
                        },
                        {
                            "name": "no_of_general_formats",
                            "type": "int",
                            "values": len(gen_format_items),
                        },

                    ]
                },
                "properties": {

                    {
                        "name": "general_formats",
                        "type": "array",
                        "values": gen_format_items,


                    },
                    {
                        "name": "Lowest Value",
                        "type": "string",
                        "values": query_data.lowest_value,
                    },
                    {
                        "name": "Highest Value",
                        "type": "string",
                        "values": query_data.highest_value,
                    },
                    {
                        "name": "Median Value",
                        "type": "string",
                        "values": query_data.median_value,
                    },
                    {
                        "name": "Least Frequent Value",
                        "type": "string",
                        "values": query_data.freq_lowest_value,
                    },
                    {
                        "name": "Lowest Frequency",
                        "type": "string",
                        "values": query_data.freq_lowest_count,
                    },
                    {
                        "name": "Lowest Frequency percentage",
                        "type": "string",
                        "values": query_data.freq_lowest_perc,
                    },
                    {
                        "name": "Highest Frequent Value",
                        "type": "string",
                        "values": query_data.freq_highest_value_value,
                    },
                    {
                        "name": "Highest Frequency",
                        "type": "string",
                        "values": query_data.freq_highest_count_count,
                    },
                    {
                        "name": "highest Frequency percentage",
                        "type": "string",
                        "values": query_data.freq_highest_perc,
                    },

                }
            },

            {
                "name": "domain_values",
                "type": "array",
                "fields": [
                    {
                        "name": "top_25",
                        "type": "array",
                        "value": [],
                    },
                    {
                        "name": "bottom_25",
                        "type": "array",
                        "value": [],
                    }
                ],

            },

        ]
    }


    return data



print(schema())
