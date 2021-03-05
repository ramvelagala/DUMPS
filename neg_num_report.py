{
    "name": "Report",

    "type": "record",

    "fields": [
        {
            "name": "Column",
            "type": "string",
            "doc": "name of the column.",
            "default": " "
        },
        {
            "name": "inferred_data_class",
            "type": "string",
            "doc" : "inferred data class of the column."

        },

        {
            "name": "MetaData",
            "type": {
                "name": "MetaData",
                "type": "record",
                "fields": [{
                    "name": "Defined",
                    "type": {
                        "name": "Defined",
                        "type": "record",
                        "doc": "Meta data that is defined on column level.",
                        "fields": [
                            {
                                "name": "Data_type",
                                "type": "string",
                                "doc": ""

                            },
                            {
                                "name": "data_length",
                                "type": "int",
                                "doc": ""

                            },
                            {
                                "name": "Precision",
                                "type": "int",
                                "doc": ""

                            },
                            {
                                "name": "scale",
                                "type": "int"

                            },
                            {
                                "name": "data_scale",
                                "type": "int",
                                "doc": ""

                            }
                            ]
                    }
                },

                    {
                        "name": "Inferred",
                        "type": {
                            "name": "Inferred",
                            "type": "record",
                            "doc": "Meta data that we have in column level.",
                            "fields": [
                            {
                                "name": "Data_type",
                                "type": "string",
                                "doc": ""

                            },
                            {
                                "name": "data_length",
                                "type": "int",
                                "doc": ""

                            },
                            {
                                "name": "Precision",
                                "type": "int",
                                "doc": ""

                            },
                            {
                                "name": "scale",
                                "type": "int",
                                "doc": ""

                            },
                            {
                                "name": "data_scale",
                                "type": "int",
                                "doc": ""

                            }
                            ]
                        }
                    }
                ]
            }
        },
        {
            "name": "domain_highlights",
            "type":
            [
            {
                "name": "domain_highlights",
                "type": "record",
                "doc": "Contains all the aggregations in the report.",
                "fields": [
                    {
                        "name": "TotalRows",
                        "type": "int",
                        "doc": "Total number of rows in the column."

                    },
                    {
                        "name": "Cardinality",
                        "type": "int",
                        "doc": "Number of unique/distinct values in the column."

                    },
                    {
                        "name": "per_of_Cardinality",
                        "type": "float",
                        "doc": "Percentage of unique values in the column."


                    },
                    {
                        "name": "Nulls",
                        "type": "int",
                        "doc": "Number of nulls in the column."

                    },
                    {
                        "name": "no_of_general_formats",
                        "type": "int",
                        "items": "string",
                        "doc": "Total number of unique formats in column."

                    },
                    {
                    "name": "general_formats",
                    "doc": "List of different types of general formats in the column.",
                    "type":
					{
						"type": "array",
						"items": "string",
						"name": "general_formats"
					}


                    },
                    {
                        "name": "Lowest_Value",
                        "type": "string",
                        "doc": "Lowest value in the column."

                    },
                    {
                        "name": "Highest_Value",
                        "type": "string",
                        "doc": "Highest value in the column."

                    },
                    {
                        "name": "Median_Value",
                        "type": "string",
                        "doc": "Median value in the column."

                    },
                    {
                        "name": "Least_Frequent_Value",
                        "type": "string",
                        "doc": "Least frequent value in the column."

                    },
                    {
                        "name": "Lowest_Frequency",
                        "type": "int",
                        "doc": "frequency of the lowest frequency value in the column."

                    },
                    {
                        "name": "Lowest_Frequency_percentage",
                        "type": "float",
                        "doc": "percentage of frequency of the lowest frequency value in the column."

                    },
                    {
                        "name": "Highest_Frequent_Value",
                        "type": "int",
                        "doc": "Highest frequent value in the column."

                    },
                    {
                        "name": "Highest_Frequency",
                        "type": "int",
                        "doc": "frequency of the lowest frequency value in the column."

                    },
                    {
                        "name": "highest_Frequency_percentage",
                        "type": "float",
                        "doc": "percentage of frequency of the lowest frequency value in the column."

                    }

                ]
            }
            ]

        },
		{
            "name": "domain_values",
            "type":
            [
            {
                "name": "domain_values",
                "type": "record",
                "fields":
				[
				{

                    "name": "bot_25",
                    "doc": "bot 25 % (quarter) of distinct values in array",
                    "type":
						{
							"type": "array",
							"items":
							    {
							        "type": "record",
                                    "name": "bot_25_perc_unqiue",
                                    "fields":
                                    [
                                    {
                                        "name": "bot_25_unique_values",
                                        "doc": "bot 25 % (quarter) of distinct values in array.",
                                        "type": "double"
                                    },
                                    {
                                        "name": "freq_bot_25_unique_values",
                                        "doc": "Frequency of bot 25 % (quarter) of distinct values in array.",
                                        "type": "int"
                                    },

                                    {
                                        "name": "perc_freq_bot_25_unique_values",
                                        "doc": "Percentage of frequency of bot 25 % (quarter) of distinct values in array.",
                                        "type": "float"
                                    }
                                    ]
							    }
						}

				},
				{

                    "name": "top_25",
                    "doc": "top 25 % (quarter) of distinct values in array",
                    "type":
						{
							"type": "array",
							"items":
							    {
							        "type": "record",
                                    "name": "top_25_perc_unqiue",
                                    "fields":
                                    [
                                    {
                                        "name": "top_25_unique_values",
                                        "doc": "top 25 % (quarter) of distinct values in array.",
                                        "type": "double"
                                    },
                                    {
                                        "name": "freq_top_25_unique_values",
                                        "doc": "Frequency of top 25 % (quarter) of distinct values in array.",
                                        "type": "int"
                                    },

                                    {
                                        "name": "perc_freq_top_25_unique_values",
                                        "doc": "Percentage of frequency of top 25 % (quarter) of distinct values in array.",
                                        "type": "float"
                                    }
                                    ]
							    }
						}

				}


				]

			}
			]
		}


    ]
}
