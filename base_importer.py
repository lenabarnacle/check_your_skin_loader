from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datamodels.msg_log import entity_msg_log
from datetime import datetime

class base_importer():
    def connect(self, connection_string):
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def disconnect(self):
        self.Session.close_all()

    def add_log_entry(self, table):
        current_time = datetime.now()

        entry = entity_msg_log(table_name=table,
                               record_count=None,
                               upload_start_time=current_time,
                               upload_end_time=None,
                               status=None)

        self.session.add(entry)
        self.session.flush()
        result = entry.id
        self.session.commit()
        return result

    def update_log_entry(self, msg_id, status, count):
        current_time = datetime.now()

        self.session.query(entity_msg_log).filter(entity_msg_log.id == msg_id). \
            update({"status": status, "upload_end_time": current_time, "record_count": count})
        self.session.commit()

        return 1
