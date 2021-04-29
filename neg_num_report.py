import json

with open("testing.json") as f:
    data = json.load(f)

print(f"data: {data}")
# print(data.items())
# values = data.get('data')
# print(f"data.getmethod : {values}")
# print(f"number of tables: {len(values)}")
for num, val in enumerate(data.values()):
    print(val)
    columns = val.get("column")
    print(len(columns))
    ctx_date_start = val.get("datestart")
    ctx_date_start = val.get("dateend")
    ctx_date_start = val.get("limit")
    ctx_date_start = val.get("focus_values")
print(f"number of tables: {num+1}")
#     for key, value in val.items():
#         table = key
#         print(f"Table: {table}")
#         okish = value
#         print(f"okish: {okish}")
#         for value in okish:
#             print(value)
