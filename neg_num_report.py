from jinja2 import Environment, PackageLoader, FileSystemLoader
import os, json
from pathlib import Path

# print(os.path.abspath(_file_))


root = os.path.dirname(os.path.abspath(_file_))
template_dir = os.path.join(root, 'app/templates')
env = Environment(loader=FileSystemLoader(template_dir))
template = env.get_template('profiling.html')
filename = os.path.join(root, 'html', 'profile.html')
json_data = os.path.join(root, 'html', 'result.json')
domain_metric_data = card = uniq = None
with open(json_data) as f:
data = json.load(f)
  # print(data, type(data))
    result = data.get('result')
      if result:
        # print(result, type(result))
          for i in result:
          rows = i.get('rows')
            if rows is not None:
              if rows != 0:
                print("no of rows - ", rows)
                domain_metric_data = rows
                card = i.get('cardinality')
                uniq = i.get('uniqueness')
                  else:
                    print("no of rows - 0")

with open(filename, 'w') as fh:
fh.write(template.render(
  title="Data Profiling",
    h1="Data Element Profiler",
      names=["Foo", "Bar", "Qux"],
        dom_data = domain_metric_data,
          cardinality = card,
            uniqueness = uniq
              # json_data=json_data
                ))
