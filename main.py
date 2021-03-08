
import copy
import json
import avro
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader

def avro_gen():


    schema = {
      "type" : "record",
      "name" : "MyClass",
      "namespace" : "com.test.avro",
      "fields" : [
          {
            "name" : "tablename",
            "type" : "string"
            },
          {
            "name" : "elements",
            "type" : {
              "type" : "array",
              "items" : "string"
            }
            }
        ]
    }

    data = {'tablename': 'Pierre-Simon Laplace', 'elements': ['col2', 'dfdfd']}


    # Parse the schema so we can use it to write the data
    schema_parsed = avro.schema.parse(json.dumps(schema))

    # Write data to an avro file
    with open('C:\\Users\\schitturi\\Desktop\\swamy\\users.avro', 'wb') as f:
        writer = DataFileWriter(f, DatumWriter(), schema_parsed)
        writer.append({'tablename': 'Pierre-Simon Laplace', 'elements': list(['col2', 'dfdfd'])})
        # writer.append({'tablename': 'DAta Profile', 'elements': list(['some_sk', 'few_sk'])})
        writer.close()
    with open('C:\\Users\\schitturi\\Desktop\\swamy\\users.avro', 'rb') as f:
        reader = DataFileReader(f, DatumReader())
        users = [user for user in reader]
        reader.close()

    # print(f'Users:\n {users}')
    # dictionaries comparision
    # print(cmp(data, users))
    if users[0] == data:
        print("its true")
    else:
        print("not;;;;;;;;;;;;;")
    # print(result)
    print(users[0])
    print(data)

    return [data, users]



avro_gen()
