import datetime
import click
import requests
from xml.etree import ElementTree

date_format = '%d/%m/%Y'


@click.command()
@click.option('--sum', prompt='Sum of rubles', help='Amount of rubles to convert')
@click.option('--date', default=str(datetime.datetime.today().strftime(date_format)),
              prompt='Convert dates (press Enter for today)',
              help="The list of convert dates (format DD/MM/YYYY), if many values they should be space-separated."
                   "If no date was provided today's would be assumed")
@click.option('--currencies', default='USD', prompt='Convert currencies (press Enter for USD)',
              help="The list of currencies to convert")
def convert(sum, date, currencies):
    request_dates = date.split(' ')
    currencies = currencies.split(' ')
    for each in request_dates:
        try:
            datetime.datetime.strptime(each, date_format)
        except:
            click.echo(f'ERROR: Incorrect date format for {each}')
            return None
        click.echo(f'-------DATE {each}----------')
        resp = requests.get('https://www.cbr.ru/scripts/XML_daily.asp', params={'date_req': each})
        tree = ElementTree.fromstring(resp.content)
        for elem in tree:
            cur_char_code = elem.find('CharCode').text
            if cur_char_code in currencies:
                cur_value = elem.find('Value').text
                cur_nominal = elem.find('Nominal').text
                click.echo(
                    f'{sum} rub equals {float(sum) / (float(cur_value.replace(",", ".")) / float(cur_nominal))} {cur_char_code}')
        click.echo('\n')


if __name__ == '__main__':
    convert()
