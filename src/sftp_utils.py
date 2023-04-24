from datetime import datetime, timedelta
import data_connections as data_connections

import paramiko.client
import sqlalchemy.orm
import pandas as pd
import sqlalchemy
import paramiko

from typing import Optional, List, Tuple
import os


mapper_registry = sqlalchemy.orm.registry()
Base = sqlalchemy.orm.declarative_base()

sftp_host = os.getenv("SFTP_HOST")
sftp_user = os.getenv("SFTP_USER")
sftp_password = os.getenv("SFTP_PASSWORD")
sftp_port = int(os.getenv("SFTP_PORT", "22"))

sol3_sftp_host = os.getenv("SOL3_SFTP_HOST")
sol3_sftp_user = os.getenv("SOL3_SFTP_USER")
sol3_sftp_password = os.getenv("SOL3_SFTP_PASSWORD")
sol3_sftp_port = int(os.getenv("SOL3_SFTP_PORT", "22"))

# rjo_sftp_host = os.getenv("RJO_SFTP_HOST")
# rjo_sftp_user = os.getenv("RJO_SFTP_USER")
# rjo_sftp_password = os.getenv("RJO_SFTP_PASSWORD")
# rjo_sftp_port = int(os.getenv("RJO_SFTP_PORT", "22"))

rjo_sftp_host = os.getenv("RJO_SFTP_HOST","sftp.rjobrien.com")
rjo_sftp_user = os.getenv("RJO_SFTP_USER","UPETRADING")
rjo_sftp_password = os.getenv("RJO_SFTP_PASSWORD","3BJB3hpTw4qBH68")
rjo_sftp_port = int(os.getenv("RJO_SFTP_PORT", "22"))


class CounterpartyClearerNotFound(Exception):
    counterparty = ""


@mapper_registry.mapped
class CounterpartyClearer:
    __tablename__ = "counterparty_clearer"

    counterparty = sqlalchemy.Column(sqlalchemy.Text, primary_key=True)
    clearer = sqlalchemy.Column(sqlalchemy.Text)


class RoutedTrade(Base):
    __tablename__ = "routed_trades"

    routing_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    datetime = sqlalchemy.Column(sqlalchemy.DateTime)
    sender = sqlalchemy.Column(sqlalchemy.Text)
    state = sqlalchemy.Column(sqlalchemy.Text)
    broker = sqlalchemy.Column(sqlalchemy.Text)
    error = sqlalchemy.Column(sqlalchemy.Text)


def add_routing_trade(
    datetime: datetime, sender: str, counterparty: str, error: Optional[str] = None
) -> RoutedTrade:
    pg_engine = data_connections.PostGresEngine()
    RoutedTrade.metadata.create_all(pg_engine)
    with sqlalchemy.orm.Session(pg_engine) as session:
        routing_trade = RoutedTrade(
            datetime=datetime, sender=sender, state="UNSENT", broker=counterparty
        )
        if error is not None:
            routing_trade.error = error
        session.add(routing_trade)
        session.commit()

    return routing_trade


def update_routing_trade(
    routing_trade: RoutedTrade,
    state: str,
    datetime: Optional[datetime] = None,
    counterparty: Optional[str] = None,
    error: Optional[str] = None,
):
    with sqlalchemy.orm.Session(data_connections.PostGresEngine()) as session:
        session.add(routing_trade)
        routing_trade.state = state
        if datetime is not None:
            routing_trade.datetime = datetime
        if counterparty is not None:
            routing_trade.broker = counterparty
        routing_trade.error = error
        session.commit()
    return routing_trade


def submit_to_stfp(
    destination_dir: str, destination_file_name: str, local_file_loc: str
):
    """Submits a file to the environment variable defined SFTP server.

    :param destination_dir: Destination directory to upload to.
    :type destination_dir: str
    :param destination_file_name: Name of the file when uploaded.
    :type destination_file_name: str
    :param local_file_loc: Local file location, including file name.
    :type local_file_loc: str
    """
    with paramiko.client.SSHClient() as ssh_client:
        ssh_client.load_host_keys("./known_hosts")
        ssh_client.connect(
            rjo_sftp_host,
            port=rjo_sftp_port,
            username=rjo_sftp_user,
            password=rjo_sftp_password,
        )

        sftp = ssh_client.open_sftp()
        sftp.chdir(destination_dir)
        sftp.put(local_file_loc, destination_file_name)
        sftp.close()


def get_clearer_from_counterparty(counterparty: str) -> Optional[str]:
    with data_connections.PostGresEngine().connect() as connection:
        query = sqlalchemy.select(CounterpartyClearer.clearer).where(
            CounterpartyClearer.counterparty == counterparty.upper()
        )
        clearer = connection.execute(query)
        clearer = clearer.first()
    if clearer is None:
        raise CounterpartyClearerNotFound(
            f"Counterparty `{counterparty}` not found in database.",
            counterparty=counterparty,
        )
    return clearer[0]


def fetch_latest_sol3_export(
    file_type: str, file_format: str
) -> Tuple[pd.DataFrame, str]:
    with paramiko.client.SSHClient() as ssh_client:
        ssh_client.load_host_keys("./known_hosts")
        ssh_client.connect(
            sol3_sftp_host,
            port=sol3_sftp_port,
            username=sol3_sftp_user,
            password=sol3_sftp_password,
        )

        sftp = ssh_client.open_sftp()
        if file_type == "positions":
            sftp.chdir("/trades/new/exports")
        else:  # to get daily trades file
            sftp.chdir("/trades/new")

        now_time = datetime.utcnow()
        sftp_files: List[Tuple[str, datetime]] = []  # stored as (filename, datetime)
        for filename in sftp.listdir():
            try:
                file_datetime = datetime.strptime(filename, file_format)
            except ValueError:
                continue
            sftp_files.append((filename, file_datetime))

        most_recent_sftp_filename: str = sorted(
            sftp_files,
            key=lambda file_tuple: (now_time - file_tuple[1]).total_seconds(),
        )[0][0]

        with sftp.open(most_recent_sftp_filename) as f:
            most_recent_sol3_pos_df = pd.read_csv(f, sep=";")

    return [most_recent_sol3_pos_df, most_recent_sftp_filename]


# function to fetch any file from the RJO SFTP server using filename format
def fetch_latest_rjo_export(file_format: str) -> Tuple[pd.DataFrame, str]:
    with paramiko.client.SSHClient() as ssh_client:
        ssh_client.load_host_keys("./known_hosts")
        ssh_client.connect(
            rjo_sftp_host,
            port=rjo_sftp_port,
            username=rjo_sftp_user,
            password=rjo_sftp_password,
        )

        sftp = ssh_client.open_sftp()
        sftp.chdir("/OvernightReports")

        now_time = datetime.utcnow()
        sftp_files: List[Tuple[str, datetime]] = []  # stored as (filename, datetime)
        for filename in sftp.listdir():
            try:
                file_datetime = datetime.strptime(filename, f"{file_format}")
            except ValueError:
                # print(f"{filename} did not match normal file name format")
                continue
            sftp_files.append((filename, file_datetime))

        most_recent_sftp_filename: str = sorted(
            sftp_files,
            key=lambda file_tuple: (now_time - file_tuple[1]).total_seconds(),
        )[0][0]

        with sftp.open(most_recent_sftp_filename) as f:
            most_recent_rjo_cme_pos_export = pd.read_csv(f, sep=",")

    return (most_recent_rjo_cme_pos_export, most_recent_sftp_filename)


# function to download a PDF from the RJO SFTP server using filename format
def download_rjo_statement(rjo_date: str) -> str:
    with paramiko.client.SSHClient() as ssh_client:
        ssh_client.load_host_keys("./known_hosts")
        ssh_client.connect(
            rjo_sftp_host,
            port=rjo_sftp_port,
            username=rjo_sftp_user,
            password=rjo_sftp_password,
        )

        sftp = ssh_client.open_sftp()
        sftp.chdir("/OvernightReports")

        pdf_filename = f"UPETRADING_statement_dstm_{rjo_date}.pdf"
        filepath = f"./assets/{pdf_filename}"
        found_file = False
        for filename in sftp.listdir():
            if filename == pdf_filename:
                sftp.get(pdf_filename, filepath)
                found_file = True
                break

        return filepath if found_file else None
