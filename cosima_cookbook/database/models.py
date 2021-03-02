from pathlib import Path

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy import MetaData, Table, select, sql, exists
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from .utils import UniqueMixin

__all__ = ["Base", "NCExperiment", "Keyword", "NCFile", "CFVariable", "NCVar"]

Base = declarative_base()

keyword_assoc_table = Table(
    "keyword_assoc",
    Base.metadata,
    Column("expt_id", Integer, ForeignKey("experiments.id")),
    Column("keyword_id", Integer, ForeignKey("keywords.id")),
)


class NCExperiment(Base):
    __tablename__ = "experiments"
    # composite index since an experiment name may not be unique
    __table_args__ = (
        Index(
            "ix_experiments_experiment_rootdir", "experiment", "root_dir", unique=True
        ),
    )

    id = Column(Integer, primary_key=True)

    #: Experiment name
    experiment = Column(String, nullable=False)
    #: Root directory containing 'output???' directories
    root_dir = Column(String, nullable=False)

    # Other experiment metadata (populated from metadata.yaml)
    metadata_keys = ["contact", "email", "created", "description", "notes", "keywords"]
    contact = Column(String)
    email = Column(String)
    created = Column(DateTime)
    #: Human-readable experiment description
    description = Column(Text)
    #: Any other notes
    notes = Column(Text)
    #: Short, categorical keywords
    kw = relationship(
        "Keyword",
        secondary=keyword_assoc_table,
        back_populates="experiments",
        cascade="merge",  # allow unique constraints on uncommitted session
        collection_class=set,
    )
    # add an association proxy to the keyword column of the keywords table
    # this lets us add keywords as strings rather than Keyword objects
    keywords = association_proxy("kw", "keyword")

    #: Files in this experiment
    ncfiles = relationship(
        "NCFile", back_populates="experiment", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return "<NCExperiment('{e.experiment}', '{e.root_dir}', {} files)>".format(
            len(self.ncfiles), e=self
        )


class Keyword(UniqueMixin, Base):
    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True)
    # enable sqlite case-insensitive string collation
    _keyword = Column(
        String(collation="NOCASE"), nullable=False, unique=True, index=True
    )

    # hybrid property lets us define different behaviour at the instance
    # and expression levels: for an instance, we return the lowercased keyword
    @hybrid_property
    def keyword(self):
        return self._keyword.lower()

    @keyword.setter
    def keyword(self, keyword):
        self._keyword = keyword

    # in an expression, because the column is 'collate nocase', we can just
    # use the raw keyword
    @keyword.expression
    def keyword(cls):
        return cls._keyword

    experiments = relationship(
        "NCExperiment", secondary=keyword_assoc_table, back_populates="kw"
    )

    def __init__(self, keyword):
        self.keyword = keyword

    @classmethod
    def unique_hash(cls, keyword):
        return keyword

    @classmethod
    def unique_filter(cls, query, keyword):
        return query.filter(Keyword.keyword == keyword)


class NCFile(Base):
    __tablename__ = "ncfiles"
    __table_args__ = (
        Index("ix_ncfiles_experiment_ncfile", "experiment_id", "ncfile", unique=True),
    )

    id = Column(Integer, primary_key=True)

    #: When this file was indexed
    index_time = Column(DateTime)
    #: The file name
    ncfile = Column(String, index=True)
    #: Is the file actually present on the filesystem?
    present = Column(Boolean)
    #: The experiment to which the file belongs
    experiment_id = Column(
        Integer, ForeignKey("experiments.id"), nullable=False, index=True
    )
    experiment = relationship("NCExperiment", back_populates="ncfiles")
    #: Start time of data in the file
    time_start = Column(String)
    #: End time of data in the file
    time_end = Column(String)
    #: Temporal frequency of the file
    frequency = Column(String)

    #: variables in this file
    ncvars = relationship(
        "NCVar", back_populates="ncfile", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return """<NCFile('{e.ncfile}' in {e.experiment}, {} variables, \
from {e.time_start} to {e.time_end}, {e.frequency} frequency, {}present)>""".format(
            len(self.ncvars), "" if self.present else "not ", e=self
        )

    @property
    def ncfile_path(self):
        return Path(self.experiment.root_dir) / Path(self.ncfile)


class CFVariable(UniqueMixin, Base):
    __tablename__ = "variables"
    __table_args__ = (
        Index("ix_variables_name_long_name", "name", "long_name", unique=True),
    )

    id = Column(Integer, primary_key=True)

    #: Attributes associated with the variable that should
    #: be stored in the database
    attributes = ["long_name", "standard_name", "units"]

    #: The variable name
    name = Column(String, nullable=False, index=True)
    #: The variable long name (CF Conventions §3.2)
    long_name = Column(String)
    #: The variable long name (CF Conventions §3.3)
    standard_name = Column(String)
    #: The variable long name (CF Conventions §3.1)
    units = Column(String)

    #: Back-populate a list of ncvars that use this variable
    ncvars = relationship("NCVar", back_populates="variable")

    def __init__(self, name, long_name=None, standard_name=None, units=None):
        self.name = name
        self.long_name = long_name
        self.standard_name = standard_name
        self.units = units

    def __repr__(self):
        return "<CFVariable('{e.name}', in {} NCVars)>".format(len(self.ncvars), e=self)

    @classmethod
    def unique_hash(cls, name, long_name, *arg):
        return "{}_{}".format(name, long_name)

    @classmethod
    def unique_filter(cls, query, name, long_name, *arg):
        return query.filter(CFVariable.name == name).filter(
            CFVariable.long_name == long_name
        )


class NCVar(Base):
    __tablename__ = "ncvars"

    id = Column(Integer, primary_key=True)

    #: The ncfile to which this variable belongs
    ncfile_id = Column(Integer, ForeignKey("ncfiles.id"), nullable=False, index=True)
    ncfile = relationship("NCFile", back_populates="ncvars")
    #: The generic form of this variable (name and attributes)
    variable_id = Column(Integer, ForeignKey("variables.id"), nullable=False)
    variable = relationship(
        "CFVariable", back_populates="ncvars", uselist=False, cascade="merge"
    )
    #: Proxy for the variable name
    varname = association_proxy("variable", "name")
    #: Serialised tuple of variable dimensions
    dimensions = Column(String)
    #: Serialised tuple of chunking along each dimension
    chunking = Column(String)

    def __repr__(self):
        return "<NCVar('{e.varname}' in '{e.ncfile.ncfile_path}')>".format(e=self)
