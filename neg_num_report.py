{
  "type" : "record",
  "name" : "MyClass",
  "namespace" : "com.test.avro",
  "fields" : [ {
    "name" : "tablename",
    "type" : "string"
  }, {
    "name" : "elements",
    "type" : {
      "type" : "array",
      "items" : "string"
    }
  } ]
}
