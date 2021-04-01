@click.command()
@click.option("--name",'-n')
@click.option("--focus_values", '-f',multiple=True)
@click.option("--limit_top",prompt="No of Values to be displayed for highest frequent values",type=int)
def final(name,focus_values, limit_top):
    print(name)
    print(f"This is limit_top: {limit_top}")
    #print(f"This is limit_bot: {limit_bot}")
    print(f"This is focus_values:{focus_values}")
