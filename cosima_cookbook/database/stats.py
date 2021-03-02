from sqlalchemy import Table, Column, DateTime, Integer, String, ForeignKey, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select

# this base is separate from the one defined in .models -- we don't
# want to put this table into the main database
StatsBase = declarative_base()


class Accesses(StatsBase):
    __tablename__ = "accesses"

    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    user = Column(String)
    ncfile_id = Column(Integer, nullable=False, index=True)

    def __repr__(self):
        return "<Access(ncfile='{e.ncfile_id}' by '{e.user}' on {e.date}".format(e=self)


def lookup_stats_database(conn):
    """Look within an indexing database to see whether it defines a stats database."""

    # ensure the table exists
    metadata = MetaData()
    stats = Table("stats", metadata, Column("path", String))
    metadata.create_all(conn)

    # query for the path of the database
    q = conn.execute(select([stats.c.path]))
    path = q.fetchone()

    if path is None:
        return None

    return path[0]
