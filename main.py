from jinja2 import Environment, PackageLoader
import os


#print(os.path.abspath(__file__))

env = Environment(loader=PackageLoader('app'))
template = env.get_template('profiling.html')

#print(os.path.abspath(__file__))
 
root = os.path.dirname(os.path.abspath(__file__))
filename = os.path.join(root, 'html', 'profile.html')
json_data = os.path.join(root, 'html', 'result.json')
 
with open(filename, 'w') as fh:
    fh.write(template.render(
        h1 = "Hello Jinja2",
        show_one = True,
        show_two = False,
        names    = ["Foo", "Bar", "Qux"],
		json_data = json_data
    ))
