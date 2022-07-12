import data_connections

import sqlalchemy
import sqlalchemy.orm
import paramiko
import paramiko.client

from typing import Optional
import os


mapper_registry = sqlalchemy.orm.registry()


sftp_host = os.getenv("SFTP_HOST")
sftp_user = os.getenv("SFTP_USER")
sftp_password = os.getenv("SFTP_PASSWORD")
sftp_port = int(os.getenv("SFTP_PORT", "22"))


class CounterpartyClearerNotFound(Exception):
    pass


@mapper_registry.mapped
class CounterpartyClearer:
    __tablename__ = "counterparty_clearer"

    counterparty = sqlalchemy.Column(sqlalchemy.Text, primary_key=True)
    clearer = sqlalchemy.Column(sqlalchemy.Text)


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
            sftp_host,
            port=sftp_port,
            username=sftp_user,
            password=sftp_password,
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
        clearer = clearer.first()[0]
    if clearer is None:
        raise CounterpartyClearerNotFound(
            f"Counterparty {counterparty} not found in database."
        )
    return clearer
