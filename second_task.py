import click
from clickhouse_driver import Client
import datetime

client = Client(host='localhost')
date_format = '%d/%m/%Y'


@click.group()
def cli1():
    pass


@cli1.command()
@click.option('--date_from', prompt='Start period', help='Period to start counting from (format DD/MM/YYYY)')
@click.option('--date_to', prompt='End period', help='Period to finish counting (format DD/MM/YYYY)')
@click.option('--ref', prompt='Reference count',
              help="Reference group")
def installs(date_from, date_to, ref):
    """Installs"""
    try:
        date_f = int(datetime.datetime.strptime(date_from, date_format).timestamp())
        date_t = int(datetime.datetime.strptime(date_to, date_format).timestamp())
    except:
        click.echo(f'ERROR: Incorrect date format')
        return None

    count = client.execute(
        f"SELECT count() FROM default.fact_reg WHERE ref = '{ref}' AND {date_f} <= ts AND ts <= {date_t}")
    click.echo(count[0][0])


@click.group()
def cli2():
    pass


@cli2.command()
@click.option('--date_from', prompt='Start period', help='Period to start counting from (format DD/MM/YYYY)')
@click.option('--date_to', prompt='End period', help='Period to finish counting (format DD/MM/YYYY)')
@click.option('--ref', prompt='Reference count',
              help="Reference group")
def retention(date_from, date_to, ref):
    """Retention"""
    try:
        date_f = int(datetime.datetime.strptime(date_from, date_format).timestamp())
        date_t = int(datetime.datetime.strptime(date_to, date_format).timestamp())
    except:
        click.echo(f'ERROR: Incorrect date format')
        return None
    retention = client.execute(f"SELECT dateDiff('day', toDateTime(default.fact_reg.ts), toDateTime(default.fact_login.ts)) AS day,"
                           f" countDistinct(user_id) AS retention FROM default.fact_reg LEFT JOIN default.fact_login ON default.fact_reg.user_id = default.fact_login.user_id "
                           f"WHERE default.fact_reg.ts >= {date_f} AND default.fact_login.ts <= {date_t} AND default.fact_reg.ref = '{ref}'"
                           f"GROUP BY day ORDER BY day ")
    click.echo(retention)


@click.group()
def cli3():
    pass


@cli3.command()
@click.option('--date_from', prompt='Start period', help='Period to start counting from (format DD/MM/YYYY)')
@click.option('--date_to', prompt='End period', help='Period to finish counting (format DD/MM/YYYY)')
@click.option('--ref', prompt='Reference count',
              help="Reference group")
def ltv(date_from, date_to, ref):
    """LTV"""
    try:
        date_f = int(datetime.datetime.strptime(date_from, date_format).timestamp())
        date_t = int(datetime.datetime.strptime(date_to, date_format).timestamp())
    except:
        click.echo(f'ERROR: Incorrect date format')
        return None
    ltv = client.execute(f"SELECT sum(sum_usd) /count() from (SELECT user_id, sum(USD) as sum_usd FROM default.fact_reg LEFT JOIN default.fact_payment ON fact_reg.user_id = fact_payment.user_id WHERE fact_reg.ref = 'games1' AND {date_f} <= ts AND ts <= {date_t} GROUP BY user_id)")
    click.echo(ltv[0][0])


cli = click.CommandCollection(sources=[cli1, cli2, cli3])

if __name__ == '__main__':
    cli()
