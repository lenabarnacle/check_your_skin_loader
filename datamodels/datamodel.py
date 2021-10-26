from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Sequence, Table, MetaData

Base = declarative_base()

table_check_your_skin = Table("check_your_skin",
                              MetaData(schema="sa"),
                              Column("test_id", primary_key=True),
                              Column("data_category", primary_key=True),
                              Column("question_num"),
                              Column("index", primary_key=True),
                              Column("data"))

class entity_check_your_skin(Base):
    __table__ = table_check_your_skin
    test_id = table_check_your_skin.c.test_id
    data_category = table_check_your_skin.c.data_category
    question_num = table_check_your_skin.c.question_num
    index = table_check_your_skin.c.index
    data = table_check_your_skin.c.data