import data_connections

import sqlalchemy
import sqlalchemy.orm
import paramiko
import paramiko.client

from typing import Optional


mapper_registry = sqlalchemy.orm.registry()


@mapper_registry.mapped
class CounterpartyClearer:
    __tablename__ = "counterparty_clearer"

    counterparty = sqlalchemy.Column(sqlalchemy.Text, primary_key=True)
    clearer = sqlalchemy.Column(sqlalchemy.Text)


def submit_to_stfp(destination_dir, file_name, file_loc: Optional[str] = None):
    with paramiko.client.SSHClient() as ssh_client:
        ssh_client.load_host_keys("./known_hosts")
        ssh_client.connect(
            hostname="69.25.147.25", username="bgm-georgia", password="jI2Gz50eG5G9T7#V"
        )
        sftp = ssh_client.open_sftp()
        sftp.chdir(destination_dir)
        sftp.put(file_loc, file_name)
        sftp.close()


def get_clearer_from_counterparty(counterparty: str) -> str:
    with data_connections.PostGresEngine().connect() as connection:
        query = sqlalchemy.select(CounterpartyClearer.clearer).where(
            CounterpartyClearer.counterparty == counterparty.upper()
        )
        clearer = connection.execute(query)
        clearer = clearer.first()[0]
    return clearer
