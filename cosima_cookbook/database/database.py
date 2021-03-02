from datetime import datetime
import logging
import os
from pathlib import Path
import re
import subprocess
from tqdm import tqdm
import warnings

import cftime
from dask.distributed import as_completed
import netCDF4
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .. import netcdf_utils
from .models import *
from ..date_utils import format_datetime

logging.captureWarnings(True)

__DB_VERSION__ = 3
__DEFAULT_DB__ = "/g/data/ik11/databases/cosima_master.db"


def create_session(db=None, debug=False):
    """Create a session for the specified database file.

    If debug=True, the session will output raw SQL whenever it is executed on the database.
    """

    if db is None:
        db = os.getenv("COSIMA_COOKBOOK_DB", __DEFAULT_DB__)

    engine = create_engine("sqlite:///" + db, echo=debug)

    # if database version is 0, we've created it anew
    conn = engine.connect()
    ver = conn.execute("PRAGMA user_version").fetchone()[0]
    if ver == 0:
        # seems we can't use usual SQL parameter strings, so we'll just format the version in...
        conn.execute("PRAGMA user_version={}".format(__DB_VERSION__))
    elif ver < __DB_VERSION__:
        raise Exception(
            "Incompatible database versions, expected {}, got {}".format(
                ver, __DB_VERSION__
            )
        )

    Base.metadata.create_all(conn)
    conn.close()

    Session = sessionmaker(bind=engine)
    return Session()


class EmptyFileError(Exception):
    pass


def update_timeinfo(f, ncfile):
    """Extract time information from a single netCDF file: start time, end time, and frequency."""

    with netCDF4.Dataset(f, "r") as ds:
        # we assume the record dimension corresponds to time
        time_dim = netcdf_utils.find_time_dimension(ds)
        if time_dim is None:
            return None

        time_var = ds.variables[time_dim]
        has_bounds = hasattr(time_var, "bounds")

        if len(time_var) == 0:
            raise EmptyFileError(
                "{} has a valid unlimited dimension, but no data".format(f)
            )

        if not hasattr(time_var, "units") or not hasattr(time_var, "calendar"):
            # non CF-compliant file -- don't process further
            return

        # Helper function to get a date
        def todate(t):
            return cftime.num2date(t, time_var.units, calendar=time_var.calendar)

        if has_bounds:
            bounds_var = ds.variables[time_var.bounds]
            ncfile.time_start = todate(bounds_var[0, 0])
            ncfile.time_end = todate(bounds_var[-1, 1])
        else:
            ncfile.time_start = todate(time_var[0])
            ncfile.time_end = todate(time_var[-1])

        if len(time_var) > 1 or has_bounds:
            # calculate frequency -- I don't see any easy way to do this, so
            # it's somewhat heuristic
            #
            # using bounds_var gets us the averaging period instead of the
            # difference between the centre of averaging periods, which is easier
            # to work with
            if has_bounds:
                next_time = todate(bounds_var[0, 1])
            else:
                next_time = todate(time_var[1])

            dt = next_time - ncfile.time_start
            if dt.days >= 365:
                years = round(dt.days / 365)
                ncfile.frequency = "{} yearly".format(years)
            elif dt.days >= 28:
                months = round(dt.days / 30)
                ncfile.frequency = "{} monthly".format(months)
            elif dt.days >= 1:
                ncfile.frequency = "{} daily".format(dt.days)
            else:
                ncfile.frequency = "{} hourly".format(dt.seconds // 3600)
        else:
            # single time value in this file and no averaging
            ncfile.frequency = "static"

        # convert start/end times to timestamps
        ncfile.time_start = format_datetime(ncfile.time_start)
        ncfile.time_end = format_datetime(ncfile.time_end)


def index_file(ncfile_name, experiment):
    """Index a single netCDF file within an experiment by retrieving all variables, their dimensions
    and chunking.
    """

    # construct absolute path to file
    f = str(Path(experiment.root_dir) / ncfile_name)

    # try to index this file, and mark it 'present' if indexing succeeds
    ncfile = NCFile(
        index_time=datetime.now(),
        ncfile=ncfile_name,
        present=False,
        experiment=experiment,
    )
    try:
        with netCDF4.Dataset(f, "r") as ds:
            for v in ds.variables.values():
                # create the generic cf variable structure
                cfvar = CFVariable(name=v.name)

                # check for other attributes
                for att in CFVariable.attributes:
                    if att in v.ncattrs():
                        setattr(cfvar, att, v.getncattr(att))

                # fill in the specifics for this file: dimensions and chunking
                ncvar = NCVar(
                    variable=cfvar,
                    dimensions=str(v.dimensions),
                    chunking=str(v.chunking()),
                )

                ncfile.ncvars.append(ncvar)

        update_timeinfo(f, ncfile)
        ncfile.present = True
    except FileNotFoundError:
        logging.info("Unable to find file: %s", f)
    except Exception as e:
        logging.error("Error indexing %s: %s", f, e)

    return ncfile


def update_metadata(experiment, session):
    """Look for a metadata.yaml for a given experiment, and populate
    the row with any data found."""

    metadata_file = Path(experiment.root_dir) / "metadata.yaml"
    if not metadata_file.exists():
        return

    try:
        metadata = yaml.safe_load(metadata_file.open())
        for k in NCExperiment.metadata_keys:
            if k in metadata:
                v = metadata[k]

                # special case for keywords: ensure we get a list
                if k == "keywords" and isinstance(v, str):
                    v = [v]

                setattr(experiment, k, v)
    except yaml.YAMLError as e:
        logging.warning("Error reading metadata file %s: %s", metadata_file, e)

    # update keywords to be unique
    experiment.kw = {Keyword.as_unique(session, kw.keyword) for kw in experiment.kw}


class IndexingError(Exception):
    pass


def index_experiment(
    experiment_dir,
    session=None,
    client=None,
    update=False,
    prune=True,
    delete=True,
    followsymlinks=False,
):
    """Index all output files for a single experiment."""

    # find all netCDF files in the hierarchy below this directory
    files = []

    options = []
    if followsymlinks:
        options.append("-L")

    cmd = ["find", *options, experiment_dir, "-name", "*.nc"]
    proc = subprocess.run(
        cmd, encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if proc.returncode != 0:
        warnings.warn(
            "Some files or directories could not be read while finding output files: %s",
            UserWarning,
        )

    results = [s for s in proc.stdout.split()]
    files.extend(results)

    expt_path = Path(experiment_dir)
    expt = NCExperiment(
        experiment=str(expt_path.name), root_dir=str(expt_path.absolute())
    )

    # look for this experiment in the database
    q = (
        session.query(NCExperiment)
        .filter(NCExperiment.experiment == expt.experiment)
        .filter(NCExperiment.root_dir == expt.root_dir)
    )
    r = q.one_or_none()
    if r is not None:
        if update:
            expt = r
        else:
            print(
                "Not re-indexing experiment: {}\nPass `update=True` to build_index()".format(
                    expt_path.name
                )
            )
            return 0

    print("Indexing experiment: {}".format(expt_path.name))

    update_metadata(expt, session)

    # make all files relative to the experiment path
    files = {str(Path(f).relative_to(expt_path)) for f in files}

    for fobj in expt.ncfiles:
        f = fobj.ncfile
        if f in files:
            # remove existing files from list, only index new files
            files.remove(f)
        else:
            if prune:
                # prune missing files from database
                if delete:
                    session.delete(fobj)
                else:
                    fobj.present = False

    results = []

    # index in parallel or serial, depending on whether we have a client
    if client is not None:
        futures = client.map(index_file, files, experiment=expt)
        results = client.gather(futures)
    else:
        results = [index_file(f, experiment=expt) for f in tqdm(files)]

    # update all variables to be unique
    for ncfile in results:
        for ncvar in ncfile.ncvars:
            v = ncvar.variable
            ncvar.variable = CFVariable.as_unique(
                session, v.name, v.long_name, v.standard_name, v.units
            )

    session.add_all(results)
    return len(results)


def build_index(
    directories,
    session,
    client=None,
    update=False,
    prune=True,
    delete=True,
    followsymlinks=False,
):
    """Index all netcdf files contained within experiment directories.

    Requires a session for the database that's been created with the create_session() function.
    If client is not None, use a distributed client for processing files in parallel.
    May scan for only new entries to add to database with the update flag.
    If prune is True files that are already in the database but are missing from the filesystem
    will be either removed if delete is also True, or flagged as missing if delete is False.
    Symbolically linked files and/or directories will be indexed if followsymlinks is True.

    Returns the number of new files that were indexed.
    """

    if not isinstance(directories, list):
        directories = [directories]

    indexed = 0
    for directory in directories:
        indexed += index_experiment(
            directory, session, client, update, prune, delete, followsymlinks
        )

    # if everything went smoothly, commit these changes to the database
    session.commit()
    return indexed


def prune_experiment(experiment, session, delete=True):
    """Delete or mark as not present the database entries for files
    within the given experiment that no longer exist or were broken at
    index time.
    """

    expt = (
        session.query(NCExperiment)
        .filter(NCExperiment.experiment == experiment)
        .one_or_none()
    )

    if not expt:
        print("No such experiment: {}".format(experiment))
        return

    for f in expt.ncfiles:
        # check whether file exists
        if not f.ncfile_path.exists() or not f.present:

            if delete:
                session.delete(f)
            else:
                f.present = False

    session.commit()


def delete_experiment(experiment, session):
    """Completely delete an experiment from the database.

    This removes the experiment entry itself, as well as all
    of its associated files.
    """

    expt = (
        session.query(NCExperiment)
        .filter(NCExperiment.experiment == experiment)
        .one_or_none()
    )

    if expt is not None:
        session.delete(expt)
        session.commit()
