import snowflake.connector
from snowflake.snowpark.session import Session
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
import json


def get_snowflake_connection():
    connection_parameters = _get_connection_params()
    return snowflake.connector.connect(**connection_parameters)


def get_snowpark_session():
    connection_parameters = _get_connection_params()
    session = Session.builder.configs(connection_parameters).create()
    session.sql_simplifier_enabled = True
    return session


def _get_connection_params():
    connection_parameters = json.load(open("connection.json"))

    with open(connection_parameters["private_key_path"], "rb") as key:
        p_key = serialization.load_pem_private_key(key.read(), password=None, backend=default_backend())

    connection_parameters["private_key"] = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return connection_parameters
