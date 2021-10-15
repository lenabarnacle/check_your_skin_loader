from sqlalchemy import Table, Column, MetaData, Sequence
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

table_msg_log = Table('msg_log',
    MetaData(schema='md'),
    Column("id",
           Sequence("msg_log_id_seq", metadata=MetaData(schema="md")),
           primary_key=True),
    Column("table_name"),
    Column("record_count"),
    Column("upload_start_time"),
    Column("upload_end_time"),
    Column("status"))

class entity_msg_log(Base):
    __table__ = table_msg_log
    id = table_msg_log.c.id
    table_name = table_msg_log.c.table_name
    record_count = table_msg_log.c.record_count
    upload_start_time = table_msg_log.c.upload_start_time
    upload_end_time = table_msg_log.c.upload_end_time
    status = table_msg_log.c.status
