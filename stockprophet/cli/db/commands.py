import click

from stockprophet.utils import read_config, read_db_settings


@click.group()
def db_group():
    pass


# --- get --- #
@db_group.group('get')
def get_group():
    pass


@get_group.command('config')
def get_config():
    header = "{:>24}\t{:>24}".format("name", "value")
    line = "{:>24}\t{:>24}".format("=" * 24, "=" * 24)
    click.echo(line)
    click.echo(header)
    click.echo(line)
    for k, v in read_db_settings().items():
        if isinstance(v, dict) or isinstance(v, list):
            continue
        if k == 'password':
            v = len(k) * '*'
        click.echo("{:>24}\t{:>24}".format(k, v))


# --- set --- #
@db_group.group('set')
def set_group():
    pass


@set_group.command('config')
@click.option('--drivername', '-d', required=True,
              type=click.Choice(['sqlite', 'mysql', 'postgresql']), help="設定driver")
@click.option('--dbname', '-n', required=True, type=str, help='設定資料庫名稱')
@click.option('--host', '-h', type=str, help='設定主機位址')
@click.option('--port', '-p', type=int, help='設定埠號')
@click.option('--username', type=str, help='設定使用者名稱')
@click.option('--password', type=str, help='設定資料庫密碼')
def set_config(drivername, dbname, host, port, username, password):
    config = read_config()
    new_config = {}
    if drivername == 'sqlite':
        new_config['drivername'] = drivername
        new_config['database'] = dbname
        config['database'] = new_config
        config.write()
    else:
        if drivername in ['mysql', 'postgresql']:
            if host is None or port is None or username is None or password is None:
                click.echo("缺少參數")
                return

            if drivername == 'mysql':
                new_config['drivername'] = "%s+%s" % (drivername, 'pymysql')
                new_config['query'] = {'charset': 'utf8'}
            else:
                new_config['drivername'] = "%s+%s" % (drivername, 'psycopg2')

            new_config['database'] = dbname
            new_config['host'] = host
            new_config['port'] = port
            new_config['username'] = username
            new_config['password'] = password
            config['database'] = new_config
            config.write()
    click.echo("寫入 config.ini 設定")
