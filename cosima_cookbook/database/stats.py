from sqlalchemy import Table, Column, DateTime, Integer, String, ForeignKey, MetaData
from sqlalchemy.exc import UnboundExecutionError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import sql

# this base is separate from the one defined in .models -- we don't
# want to put this table into the main database
StatsBase = declarative_base()


class NCFileAccess(StatsBase):
    __tablename__ = "accesses"
    __table_args__ = {"schema": "stats_db"}

    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    user = Column(String)

    # we'd like to make this a proper ForeignKey constraint:
    #
    # ncfile_id = Column(Integer, ForeignKey("ncfiles.id"), nullable=False, index=True)
    #
    # but we can't:
    #
    # NoReferencedTableError: Foreign key associated with column
    # 'accesses.ncfile_id' could not find table 'ncfiles' with which
    # to generate a foreign key to target column 'id'

    ncfile_id = Column(Integer, nullable=False, index=True)

    def __repr__(self):
        return "<NCFileAccess(ncfile='{e.ncfile_id}' by '{e.user}' on {e.date}".format(
            e=self
        )


def _has_stats(session):
    """Return whether a given session has an attached stats_db."""
    q = sql.exists(
        sql.select([sql.column("name")])
        .select_from(sql.text("pragma_database_list"))
        .where(sql.text("name == 'stats_db'"))
    )
    return session.query(q).scalar()


def log_accesses(session, ncfiles):
    """Log a new access to all the NCFiles in the ncfiles iterable."""

    # first, check whether we have a registered stats connection

    if not _has_stats(session):
        return

    print("logging accesses...")


def lookup_stats_database(conn):
    """Look within an indexing database to see whether it defines a stats database."""

    # ensure the table exists
    metadata = MetaData()
    stats = Table("stats", metadata, Column("path", String))
    metadata.create_all(conn)

    # query for the path of the database
    q = conn.execute(sql.select([stats.c.path]))
    path = q.fetchone()

    if path is None:
        return None

    return path[0]
