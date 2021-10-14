from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Sequence, Table, MetaData

Base = declarative_base()

table_check_your_skin = Table("table_check_your_skin",
                               MetaData(schema="sa"),
                               Column("test_id", primary_key=True),
                               Column("index"),
                               Column("data"))

class entity_check_your_skin(Base):
    __table__ = table_check_your_skin
    test_id = table_check_your_skin.c.test_id
    index = table_check_your_skin.c.index
    data = table_check_your_skin.c.data