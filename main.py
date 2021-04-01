import click


@click.group(invoke_without_command=True)
@click.option('--age', '-a', type=int)
@click.pass_context
def cli(ctx, age):
    ctx.ensure_object(dict)
    ctx.obj['age'] = age
    if ctx.invoked_subcommand is None:
        click.echo('I was invoked without subcommand')
        click.echo(f"age: {age}")
    else:
        click.echo('I am about to invoke %s' % ctx.invoked_subcommand)
        click.echo(f"age: {age}")

@click.command()
@click.option('--profession', '-p')
@click.pass_context
def shruthi(ctx,profession):
    click.echo("Shruthi is working.")
    click.echo(f"profession: {profession}")
    click.echo(ctx)
    click.echo(f'age:{ctx.obj["age"]}')

@click.command()  # @cli, not @click!
@click.option('--firstname', '-fn')
@click.pass_context
def sync(ctx,firstname):
    click.echo(f'syncing :{firstname}')
    print(ctx)
    click.echo('The subcommand')


@click.command()  # @cli, not @click!
@click.option('--lastname', '-ln')
@click.pass_context
def desync(ctx,lastname):
    click.echo(f'desyncing :{lastname}')
    print(ctx)
    click.echo('The subcommand')


cli.add_command(desync)
cli.add_command(sync)
cli.add_command(shruthi)

if __name__ == "__main__":
    cli(obj={})
