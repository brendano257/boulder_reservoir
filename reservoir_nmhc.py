import os
import json
import datetime as dt
from datetime import datetime

from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship


class JDict(TypeDecorator):
    """
    Serializes a dictionary for SQLAlchemy storage.
    """
    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value

MutableDict.associate_with(JDict)

class JList(TypeDecorator):
    """
    Serializes a list for SQLAlchemy storage.
    """
    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        value = json.loads(value)
        return value

MutableList.associate_with(JList)

Base = declarative_base()

log_params_list = (['filename', 'sampletime', 'sampleflow1', 'sampleflow2',
                    'sampletype', 'backflushtime', 'desorbtemp', 'flashheattime',
                    'injecttime', 'bakeouttemp', 'bakeouttime', 'carrierflow',
                    'samplenum', 'samplepressure1', 'samplepressure2', 'GCHeadP',
                    'GCHeadP1', 'WT_temp_start', 'ads_temp_start', 'samplecode',
                    'WT_temp_end', 'ads_temp_end', 'traptempFH', 'GCstarttemp',
                    'traptempinject_end', 'battvinject_end', 'trapheatoutinject_end',
                    'traptempbakeout_end', 'battvbakeout_end', 'trapheatoutbakeout_end',
                    'wthottemp', 'GCoventemp'])
                    #does not include date on purpose because it's handled in GcRun

gcrun_params_list = log_params_list + ['peaks', 'date_end', 'date_start', 'crfs', 'type']

sample_types = {0:'zero', 1:'alt_standard', 2:'standard', 3:'alt_not_sure', 5:'ambient'}
# dict of all sample numbers and corresponding type names

compound_list = ['ethane','ethene','propane','propene','i-butane','acetylene',
                'n-butane','i-pentane','n-pentane','hexane','isoprene','benzene',
                'toluene','ethyl-benzene','m&p xylene','o-xylene']  # list of all quantified compounds

compound_ecns = ({'ethane': 2, 'ethene': 1.9, 'propane': 3, 'propene': 2.9,
                'i-butane': 4, 'acetylene': 1.8, 'n-butane': 4, 'i-pentane': 5,
                'n-pentane': 5, 'hexane': 6, 'isoprene': 4.8, 'benzene': 5.7,
                'toluene': 6.7, 'ethyl-benzene': 7.7, 'm&p xylene': 7.7,
                'o-xylene': 7.7}) # expected carbon numbers for mixing ratio calcs

class Crf(Base):
    """
    A crf is a set of carbon response factors for compounds, tied to a datetime and standard.
    These are assigned to GcRuns, which allows them to be integrated.

    Parameters:

    date_start : datetime, first datetime (inclusive) that the crf should be applied for
    date_end: datetime, last datetime that the crf is valid for (exclusive)
    date_revision: datetime, the time this CRF was added into the file
    standard: str, name of the standard this crf applies to
    compounds: dict, of compounds and the corresponding crf for each

    """

    __tablename__ = 'crfs'

    id = Column(Integer, primary_key = True)
    date_start = Column(DateTime, unique = True)
    date_end = Column(DateTime, unique = True)
    revision_date = Column(DateTime)
    standard = Column(String)
    compounds = Column(MutableDict.as_mutable(JDict))

    def __init__(self, date_start, date_end, date_revision, compounds, standard):
        self.date_start = date_start
        self.date_end = date_end
        self.date_revision = date_revision
        self.standard = standard
        self.compounds = compounds # assign whole dict of CRFs

    def __str__(self):
        return f'<crf {self.standard} for {self.date_start} to {self.date_end}>'

    def __repr__(self):
        return f'<crf {self.standard} for {self.date_start} to {self.date_end}>'


class Peak(Base):
    """
    A peak is just that, a signal peak in PeakSimple, Agilent, or another
    chromatography software.
    name: str, the compound name (if identified)
    mr: float, the mixing ratio (likely in ppbv) for the compound, if calculated; None if not
    pa: float, representing the area under the peak as integrated
    rt: float, retention time in minutes of the peak as integrated
    rev: int, represents the # of changes made to this peak's value
    qc: int, 0 = unreviewed, ...,  1 = final
    flag: int,
    int_notes,
    """

    __tablename__ = 'peaks'

    id = Column(Integer, primary_key = True)
    name = Column(String)
    pa = Column(Float)
    mr = Column(Float)
    rt = Column(Float)
    rev = Column(Integer)
    qc = Column(Integer)

    line_id = Column(Integer, ForeignKey('nmhclines.id'))
    correction_id = Column(Integer, ForeignKey('nmhc_corrections.correction_id'))
    log_id = Column(Integer, ForeignKey('logfiles.id'))

    def __init__(self, name, pa, rt):
        self.name = name.lower()
        self.pa = pa
        self.mr = None
        self.rt = rt
        self.rev = 0
        self.qc = 0

    def __str__(self):
        # Print the name, pa, and rt of a peak when called
        return f'<name: {self.name} pa: {self.pa} rt: {self.rt}, mr: {self.mr}>'

    def __repr__(self):
        # Print the name, pa, and rt of a peak when called
        return f'<name: {self.name} pa: {self.pa} rt: {self.rt}, mr: {self.mr}>'

    def get_name(self):
        return self.name

    def get_pa(self):
        return self.pa

    def get_rt(self):
        return self.rt

    def get_mr(self):
        return self.mr

    def set_name(self, name):
        self.name = name

    def set_pa(self, pa):
        self.pa = pa

    def set_rt(self, rt):
        self.rt = rt

    def set_mr(self, mr):
        self.mr = mr


class NmhcLine(Base):
    """
    A line in NMHC_PA.LOG, which contains a datetime and some set of peaks.

    date: datetime, from the Python datetime library representing the time it was recorded by PeakSimple
    peaklist: list, a list of all the peak objects contained in the nmhc line.
    status: str, assigned as single to start, and when matched to a log will be 'married'
    """

    __tablename__ = 'nmhclines'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime, unique=True)
    peaklist = relationship('Peak', order_by=Peak.id)
    status = Column(String)

    run_con = relationship('GcRun', uselist=False, back_populates='nmhc_con')
    nmhc_corr_con = relationship('NmhcCorrection', uselist=False, back_populates='nmhcline_con')
    nmhc_corr_id = Column(Integer, ForeignKey('nmhc_corrections.correction_id'))

    def __init__(self, date, peaks):
        self.date = date
        self.peaklist = peaks
        self.status = 'single' # all logs begin unmatched

    def get_date(self):
        return self.date

    def get_peaks(self):
        return self.peaklist

    def __str__(self):
        iso = self.date.isoformat(' ')
        return f'<NmhcLine for {iso}>'

    def __repr__(self):
        iso = self.date.isoformat(' ')
        return f'<NmhcLine for {iso}>'


class NmhcCorrection(NmhcLine):
    """
    A subclass of NmhcLine, this is linked to one NmhcLine, and is tied to a new table. All corrections are therefore
    also objects and recorded.

    status: str, either 'unapplied' or 'applied', rather than single/married as normal NmhcLines are
    """

    __tablename__ = 'nmhc_corrections'

    correction_id = Column(Integer, primary_key=True)
    peaklist = relationship('Peak', order_by=Peak.id)

    res_flag = Column(Integer)  # flag from the column in ambient_results_master.xlsx
    flag = Column(Integer)  # undetermined flagging system...

    nmhcline_con = relationship(NmhcLine, uselist=False, back_populates='nmhc_corr_con')
    correction_date = association_proxy('nmhcline_con', 'date')  # pass date from line to here

    def __init__(self, nmhcline, peaks, res_flag, flag):
        # super().__init__(nmhcline.date, peaks) # nmhcline.date, peaks
        self.peaklist = peaks
        self.nmhcline_con = nmhcline
        self.status = 'unapplied'  # all corrections are created as unapplied
        self.res_flag = res_flag
        self.flag = flag

    def __str__(self):
        return f'<NmhcCorrection for {self.correction_date} with {len(self.peaklist)} peaks>'

    def __repr__(self):
        return f'<NmhcCorrection for {self.correction_date} with {len(self.peaklist)} peaks>'

class LogFile(Base):
    """
    TODO: write all descriptions with datatypes

    The output from the LabView VI, it consists of all the listed parameters
    recorded in the file.

    filename: str, the name of the logfile in question
    date: datetime, the start of the 10-minute sampling period
    sampletime: float,
    sampleflow1: float,
    sampleflow2: float,
    sampletype: int,
    desorbtemp: float,
    flashheattime: float,
    injecttime: float,
    bakeouttemp: float,
    bakeouttime: float,
    carrierflow: float,
    samplenum: int,
    samplepressure1: float,
    samplepressure2: float
    GCHeadP: float,
    GCHeadP1: float,
    WT_temp_start: float,
    ads_temp_start: float,
    samplecode: int,
    WT_temp_end: float,
    ads_temp_end: float,
    traptempFH: float,
    GCstarttemp: float,
    traptempinject_end: float,
    battvinject_end: float,
    trapheatoutinject_end: float,
    traptempbakeout_end: float,
    battvbakeout_end: float,
    trapheatoutbakeout_end: float,
    wthottemp: float,
    GCoventemp: float,
    status: str, can be 'single' or 'married'
        A married status indicates the log has been matched to a pa line and is
        part of a GcRun object; single indicates unmatched/available for matching

    """

    __tablename__ = 'logfiles'

    id = Column(Integer, primary_key = True)
    filename = Column(String)
    date = Column(DateTime, unique = True)
    sampletime = Column(Float)
    sampleflow1 = Column(Float)
    sampleflow2 = Column(Float)
    sampletype = Column(Integer)
    backflushtime = Column(Float)
    desorbtemp = Column(Float)
    flashheattime = Column(Float)
    injecttime = Column(Float)
    bakeouttemp = Column(Float)
    bakeouttime = Column(Float)
    carrierflow = Column(Float)
    samplenum = Column(Integer)
    samplepressure1 = Column(Float)
    samplepressure2 = Column(Float)
    GCHeadP = Column(Float)
    GCHeadP1 = Column(Float)
    WT_temp_start = Column(Float)
    ads_temp_start = Column(Float)
    samplecode = Column(Integer)
    WT_temp_end = Column(Float)
    ads_temp_end = Column(Float)
    traptempFH = Column(Float)
    GCstarttemp = Column(Float)
    traptempinject_end = Column(Float)
    battvinject_end = Column(Float)
    trapheatoutinject_end = Column(Float)
    traptempbakeout_end = Column(Float)
    battvbakeout_end = Column(Float)
    trapheatoutbakeout_end = Column(Float)
    wthottemp = Column(Float)
    GCoventemp = Column(Float)
    status = Column(String)

    run_con = relationship('GcRun', uselist=False, back_populates='log_con')

    peaklist = relationship('Peak', order_by=Peak.id)

    def __init__(self, param_dict):
        # These are specified since a log_dict does not necessarily contain
            # every parameter, but all should be set, even if None
        self.filename = param_dict.get('filename', None)
        self.date = param_dict.get('date', None)
        self.sampletime = param_dict.get('sampletime', None)
        self.sampleflow1 = param_dict.get('sampleflow1', None)
        self.sampleflow2 = param_dict.get('sampleflow2', None)
        self.sampletype = param_dict.get('sampletype', None)
        self.backflushtime = param_dict.get('backflushtime', None)
        self.desorbtemp = param_dict.get('desorbtemp', None)
        self.flashheattime = param_dict.get('flashheattime', None)
        self.injecttime = param_dict.get('injecttime', None)
        self.bakeouttemp = param_dict.get('bakeouttemp', None)
        self.bakeouttime = param_dict.get('bakeouttime', None)
        self.carrierflow = param_dict.get('carrierflow', None)
        self.samplenum = param_dict.get('samplenum', None)
        self.samplepressure1 = param_dict.get('samplepressure1', None)
        self.samplepressure2 = param_dict.get('samplepressure2', None)
        self.GCHeadP = param_dict.get('GCHeadP', None)
        self.GCHeadP1 = param_dict.get('GCHeadP1', None)
        self.WT_temp_start = param_dict.get('WT_temp_start', None)
        self.ads_temp_start = param_dict.get('ads_temp_start', None)
        self.samplecode = param_dict.get('samplecode', None)
        self.WT_temp_end = param_dict.get('WT_temp_end', None)
        self.ads_temp_end = param_dict.get('ads_temp_end', None)
        self.traptempFH = param_dict.get('traptempFH', None)
        self.GCstarttemp = param_dict.get('GCstarttemp', None)
        self.traptempinject_end = param_dict.get('traptempinject_end', None)
        self.battvinject_end = param_dict.get('battvinject_end', None)
        self.trapheatoutinject_end = param_dict.get('trapheatoutinject_end', None)
        self.traptempbakeout_end = param_dict.get('traptempbakeout_end', None)
        self.battvbakeout_end = param_dict.get('battvbakeout_end', None)
        self.trapheatoutbakeout_end = param_dict.get('trapheatoutbakeout_end', None)
        self.wthottemp = param_dict.get('wthottemp', None)
        self.GCoventemp = param_dict.get('GCoventemp', None)
        self.status = 'single'

    def __str__(self):
        # Print the log's status, filename, and ISO datetime
        iso = self.date.isoformat(' ')
        return f'<{self.status} log {self.filename} at {iso}>'

    def __repr__(self):
        # Print the log's status, filename, and ISO datetime
        iso = self.date.isoformat(' ')
        return f'<{self.status} log {self.filename} at {iso}>'


class GcRun(Base):
    """
    A run, which consists of the attributes taken from the NmhcLine and LogFile
    that are used to create it. This is now a confirmed run, meaning it was executed
    in PeakSimple and the VI.

    peaks: list; of Peaks, the peaks associated with the NmhcLine associated with this file
    date_end: datetime, the date that the sample was recorded by PeakSimple
        Roughly representative of the end of the 10-minute sampling window
    date_start: datetime, the time the run-log was recorded by LabView
        Roughly representative of the start of the 10-minute sampling window
    log_params_list: See LogFile class for list (those added here do not include [date, status])

    crf: object, contains a dict of compound response factors for calculating a mixing ratio

    type: str, converted internally with a dict to give the sampletype a str-name
        {0:'blank',1:'',2:'',...7:''}

    When integrated, all the peak objects of a GcRun will gain a self.mr. These
    are kept as part of a GcRun, but references to these should be under datum.
    """

    __tablename__ = 'gcruns'

    id = Column(Integer, primary_key = True)
    type = Column(String)

    nmhcline_id = Column(Integer, ForeignKey('nmhclines.id'))
    nmhc_con = relationship('NmhcLine', uselist = False, foreign_keys=[nmhcline_id], back_populates='run_con')
    peaks = association_proxy('nmhc_con', 'peaklist')
    date_end = association_proxy('nmhc_con', 'date')  # pass date from NMHC to GcRun

    logfile_id = Column(Integer, ForeignKey('logfiles.id'))
    log_con = relationship('LogFile', uselist = False, foreign_keys=[logfile_id], back_populates='run_con')
    date_start = association_proxy('log_con', 'date')  # pass date from LogFile to GcRun

    for attr in log_params_list:
        vars()[attr] = association_proxy('log_con', attr)  # set association_proxy(s)
        # for all log parameters to pass them to this GC instance

    data_id = Column(Integer, ForeignKey('data.id'))
    data_con = relationship('Datum', uselist=False, foreign_keys=[data_id], back_populates='run_con')

    crfs = relationship('Crf', uselist=False)
    crf_id = Column(Integer, ForeignKey('crfs.id'))

    def __init__(self, LogFile, NmhcLine):
        self.nmhc_con = NmhcLine
        self.log_con = LogFile
        self.data_con = None
        self.crfs = None # begins with no crf, will be found later
        self.type = sample_types.get(self.sampletype, None)

    def __str__(self):
        return f'<matched gc run at {self.date_end}>'

    def _repr__(self):
        return f'<matched gc run at {self.date_end}>'

    def get_mr(self, compound_name):
        return next((peak.mr for peak in self.peaks if peak.name == compound_name), None)

    def get_pa(self, compound_name):
        return next((peak.pa for peak in self.peaks if peak.name == compound_name), None)

    def get_rt(self, compound_name):
        return next((peak.rt for peak in self.peaks if peak.name == compound_name), None)

    def get_unnamed_peaks(self):
        # returns list of unidentified peaks in a run
        return [peak for peak in self.peaks if peak.name == '-']

    def get_crf(self, compound_name):
        # returns the crf for the given compound as a float
        return self.crfs.compounds.get(compound_name, None)

    def integrate(self):
        if self.crfs is None:
            return None  # no crfs, no integration!
        elif self.type == 'ambient' or self.type == 'zero':
            for peak in self.peaks:
                if peak.name in compound_list and peak.name in self.crfs.compounds.keys():
                    crf = self.crfs.compounds[peak.name]

                    peak.mr = ((peak.pa/
                    (crf*compound_ecns.get(peak.name, None)*self.sampletime*self.sampleflow1))
                    *600*1)

                    # formula is (pa / (CRF * ECN * SampleTime * SampleFlow1)) * 600 *1
                    # The 600 * 1 normalizes to a sample volume of 600s by internal convention for this project

            return Datum(self)
        else:
            return None  # don't integrate if it's not an ambient or blank sample


class Datum(Base):
    """
    A point of the plural data. This is a gc run that has been integrated, has a
    mixing ratio (which can be None if it is a failed or QC removed run -- that we
    took a measurement is valuable information to report). It has a flag, revision,
    qc status, etc...

    mr: float, the mixing ratio of the gas
    unit: dict of potential units and factors to display as
    flag: object, ...
    sig_fig: dict, ...
    standard_used: object, ...
    revision: int, ...
    qc: str, ...
    notes: str, any notes about the data quality, processing, or other
    """

    __tablename__ = 'data'

    id = Column(Integer, primary_key = True)
    revision = Column(Integer)
    qc = Column(Integer)
    notes = Column(String)

    run_con = relationship('GcRun', uselist=False, back_populates='data_con')

    for attr in gcrun_params_list:
        vars()[attr] = association_proxy('run_con', attr) #set association_proxy(s)
        # for all log parameters to pass them to this GC instance

    def __init__(self, GcRun):
        self.run_con = GcRun
        self.revision = 0 # init with revision status zero
        self.qc = 0
        self.notes = None

    def __str__(self):
        return f'<data for {self.date_end} with {len(self.peaks)} peaks>'

    def __repr__(self):
        return f'<data for {self.date_end} with {len(self.peaks)} peaks>'

    # GET Methods for all embedded objects of a datum
    # These are resource-expensive, but can be used for one-offs where queries are unnecesary or tedious

    def get_mr(self, compound_name):
        return next((peak.mr for peak in self.peaks if peak.name == compound_name), None)

    def get_pa(self, compound_name):
        return next((peak.pa for peak in self.peaks if peak.name == compound_name), None)

    def get_rt(self, compound_name):
        return next((peak.rt for peak in self.peaks if peak.name == compound_name), None)

    def get_crf(self, compound_name):
        return self.crfs.compounds.get(compound_name, None)


def find_crf(crfs, sample_date):
    """
    Returns the carbon response factor object for a sample at the given sample_date

    crfs: list, of crf objects
    sample_date: datetime, the date_start attribute of a sample
    """

    return next((crf for crf in crfs if crf.date_start <= sample_date < crf.date_end), None)


def read_crf_data(filename):

    try:
        lines = open(filename).readlines()
    except FileNotFoundError:
        print('CRF File not found. No runs can be integrated.')
        return

    compounds = dict()

    keys = lines[0].split('\t')[3:] #list of strs of all compound names from file

    Crfs = []

    for line in lines[1:]:
        ls = line.split('\t')
        date_start = datetime.strptime(ls[0], '%m/%d/%Y %H:%M')
        date_end = datetime.strptime(ls[1], '%m/%d/%Y %H:%M')
        date_revision = datetime.strptime(ls[2], '%m/%d/%Y %H:%M')

        for index, (key, rf) in enumerate(zip(keys,ls[3:])):
            key = key.strip().lower()
            #unpack all names from header as keys and line items as values
            compounds[key] = float(rf)

        if date_start is not None and date_end is not None:
            Crfs.append(Crf(date_start, date_end, date_revision, compounds, 'working standard'))

    return Crfs


def read_log_file(filename):
    with open(filename) as file:
        contents = file.readlines()

        log_dict = dict()

        # There are different log file versions
            # These are the parameters shared by both, assign these, then assign
            # others based on file length
        try:
            log_dict['filename'] = file.name
            log_dict['date'] = datetime.strptime(contents[17].split('\t')[0], '%Y%j%H%M%S')
            log_dict['sampletime'] = float(contents[0].split('\t')[1])
            log_dict['sampleflow1'] = float(contents[1].split('\t')[1])
            log_dict['sampleflow2'] = float(contents[19].split('\t')[1])
            log_dict['sampletype'] = int(float(contents[2].split('\t')[1]))
            log_dict['backflushtime'] = float(contents[3].split('\t')[1])
            log_dict['desorbtemp'] = float(contents[4].split('\t')[1])
            log_dict['flashheattime'] = float(contents[5].split('\t')[1])
            log_dict['injecttime'] = float(contents[6].split('\t')[1])
            log_dict['bakeouttemp'] = float(contents[7].split('\t')[1])
            log_dict['bakeouttime'] = float(contents[8].split('\t')[1])
            log_dict['carrierflow'] = float(contents[9].split('\t')[1])
            log_dict['samplenum'] = int(float(contents[11].split('\t')[1]))
            log_dict['samplepressure1'] = float(contents[12].split('\t')[1])
            log_dict['samplepressure2'] = float(contents[18].split('\t')[1])
            log_dict['GCHeadP'] = float(contents[13].split('\t')[1])
            log_dict['WT_temp_start'] = float(contents[14].split('\t')[1])
            log_dict['ads_temp_start'] = float(contents[15].split('\t')[1])
            log_dict['samplecode'] = int(contents[17].split('\t')[0])
            log_dict['WT_temp_end'] = float(contents[20].split('\t')[1])
            log_dict['ads_temp_end'] = float(contents[21].split('\t')[1])
            log_dict['traptempFH'] = float(contents[23].split('\t')[1])
            log_dict['GCstarttemp'] = float(contents[24].split('\t')[1])
            log_dict['traptempinject_end'] = float(contents[26].split('\t')[1])

            if len(contents) == 30:
                # Early versions of the log files don't contain specific lines
                    # Don't include those missing values

                log_dict['traptempbakeout_end'] = float(contents[26].split('\t')[1])
                log_dict['wthottemp'] = float(contents[27].split('\t')[1])
                log_dict['GCHeadP1'] = float(contents[28].split('\t')[1])
                log_dict['GCoventemp'] = float(contents[29].split('\t')[1])

                return LogFile(log_dict)

            elif len(contents) == 34:

                log_dict['battvinject_end'] = float(contents[26].split('\t')[1])
                log_dict['trapheatoutinject_end'] = float(contents[27].split('\t')[1])
                log_dict['traptempbakeout_end'] = float(contents[28].split('\t')[1])
                log_dict['battvbakeout_end'] = float(contents[29].split('\t')[1])
                log_dict['trapheatoutbakeout_end'] = float(contents[30].split('\t')[1])
                log_dict['wthottemp'] = float(contents[31].split('\t')[1])
                log_dict['GCHeadP1'] = float(contents[32].split('\t')[1])
                log_dict['GCoventemp'] = float(contents[33].split('\t')[1])

                return LogFile(log_dict)
            else:
                print(f'File {file.name} had an improper number of lines and was ignored.')
                return None
        except:
            print(f'File {file.name} failed to be processed and was ignored.')
            return None

def read_pa_line(line):

    """
    read_pa_line takes one line as a str from the NMHC_PA.LOG file, and parses it
        into an NmhcLine object

    line: str, a line from NMHC_PA.LOG
    """

    ls = line.split('\t')
    line_peaks = []

    line_date = datetime.strptime(ls[1] + ' ' + ls[2], '%m/%d/%Y %H:%M:%S')

    for ind, item in enumerate(ls[3:]):

        ind = ind+3 # offset ind since we're working with ls[3:]

        peak_dict = dict()

        if '"' in item:

            peak_dict['name'] = item.strip('"') # can't fail, " is definitely there

            try:
                peak_dict['rt'] = float(ls[ind+1])
            except:
                peak_dict['rt'] = None
            try:
                peak_dict['pa'] = float(ls[ind+2])
            except:
                peak_dict['pa'] = None

            if None not in peak_dict.values():
                line_peaks.append(Peak(peak_dict['name'], peak_dict['pa'], peak_dict['rt']))

    if len(line_peaks) == 0:
        this_line = None
    else:
        this_line = NmhcLine(line_date, line_peaks)

    return this_line


def find_closest_date(date, list_of_dates):
    """
    This is a helper function that works on Python datetimes. It returns the closest date value,
    and the timedelta from the provided date.
    """
    match = min(list_of_dates, key = lambda x: abs(x - date))
    delta = match - date

    return match, delta


def search_for_attr_value(obj_list, attr, value):
    """
    Finds the first (not necesarilly the only) object in a list, where its
    attribute 'attr' is equal to 'value', returns None if none is found.
    """
    return next((obj for obj in obj_list if getattr(obj,attr, None) == value), None)


def match_log_to_pa(LogFiles, NmhcLines):
    """
    This takes a list of LogFile and NmhcLine objects and returns a list (empty, even)
        of resulting GcRun objects. When matching objects, it WILL modify their parameters
        and status if warranted.
    LogFiles: list (of LogFile objects), any log files that need partners
    NmhcLines: list (of NmhcLine objects), any NmhcLine objects that could be matched

    """

    runs = []

    for log in LogFiles:
        # For each log, attempt to find matching NmhcLine
        # unpack date attr from all NmhcLines provided
        nmhc_dates = [line.date for line in NmhcLines]

        [match, diff] = find_closest_date(log.date, nmhc_dates) # get matching date and it's difference

        if abs(diff) < dt.timedelta(minutes=11):
            # Valid match
            matched_line = search_for_attr_value(NmhcLines,'date', match) # pull matching NmhcLine

            runs.append(GcRun(log, matched_line))
            log.status = 'married'
            matched_line.status = 'married'
        else:
            continue

    return runs


def connect_to_reservoir_db(engine_str, directory):
    """
    Example:
    engine, session, Base = connect_to_reservoir_db('sqlite:///reservoir.sqlite', dir)

    Takes string name of the database to create/connect to, and the directory it should be in.

    engine_str: str, name of the database to create/connect to.
    directory: str/path, directory that the database should be made/connected to in.
        Requires context manager TempDir in order to work with async
    """
    from reservoir_nmhc import Base, TempDir

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    with TempDir(directory):
        engine = create_engine(engine_str)
    Session = sessionmaker(bind=engine)
    sess = Session()

    return engine, sess, Base

def fix_off_dates(LogFiles, NmhcLines):
    """
    Loop through a provided list of LogFile objects and correct the dates if necessary.
    A short period in the log files are mis-recorded, and a longer period in the NMHC_PA.LOG
    were also mis-recorded.

    % Correct dates for logdata - All dates prior to 13:00 MST on 4/11/2017
    % have to be corrected. Those dates were recorded as MDT, so subtracting an
    % hour will put them in MST. The first "correct" run was at 13:10 MST,
    % and the last "bad" run was at 12:50 MDT.

    % Correct dates for NMHC_PA Lines - All dates prior to 15:45 MST on 4/27/2017
    % have to be corrected. Those dates were recorded as MDT, so subtracting an
    % hour will put them in MST. The first "correct" run was at ~15:50 MST,
    % and the last "bad" run was at 14:59 MDT.

    Parameters:

    LogFiles: list, of LogFile objects
    NmhcLines: list, of NmhcLine objects
    """

    for log in LogFiles:
        if datetime(2017,3,12,2,0,0) < log.date < datetime(2017,4,11,13,0,0):
            log.date = log.date - dt.timedelta(hours = 1)

    for line in NmhcLines:
        if datetime(2017,3,12,2,0,0) < line.date < datetime(2017,4,27,15,5,0):
            line.date = line.date - dt.timedelta(hours = 1)


def check_filesize(filename):
    '''Returns filesize in bytes'''
    if os.path.isfile(filename):
        return os.path.getsize(filename)
    else:
        print(f'File {filename} not found.')
        return


class TempDir():
    """
    Context manager for working in a directory temporarily.
    """
    def __init__(self, path):
        self.old_dir = os.getcwd()
        self.new_dir = path

    def __enter__(self):
        os.chdir(self.new_dir)

    def __exit__(self, *args):
        os.chdir(self.old_dir)


def get_dates_mrs(res_session, compound, date_start=None, date_end=None):

    if date_start is None and date_end is None:
        peak_info = (res_session.query(Peak.mr, LogFile.date).filter(Peak.name == compound)
                     .join(NmhcLine).join(GcRun).join(LogFile)) # get everything
        mrs, dates = zip(*peak_info.all())
        return mrs, dates

    elif date_start is None:
        peak_info = (res_session.query(Peak.mr, LogFile.date).filter(Peak.name == compound)
                     .join(NmhcLine).join(GcRun).join(LogFile)
                     .filter(LogFile.date < date_end)) # get only before the end date given
        mrs, dates = zip(*peak_info.all())
        return mrs, dates

    elif date_end is None:
        peak_info = (res_session.query(Peak.mr, LogFile.date).filter(Peak.name == compound)
                     .join(NmhcLine).join(GcRun).join(LogFile)
                     .filter(LogFile.date > date_start))
        mrs, dates = zip(*peak_info.all()) # get only after the start date given
        return mrs, dates

    else:
        peak_info = (res_session.query(Peak.mr, LogFile.date).filter(Peak.name == compound)
                     .join(NmhcLine).join(GcRun).join(LogFile)
                     .filter(LogFile.date.between(date_start, date_end))) # get between date bookends (inclusive beginning!)
        mrs, dates = zip(*peak_info.all())
        return mrs, dates


def res_nmhc_plot(dates, compound_dict, limits=None, minor_ticks=None, major_ticks=None):
    """
    Versatile dat plotter for the project with a dynamic duration/tick scheme for web-ready plots.

    Example with all dates supplied:
        res_nmhc_plot((None, {'Ethane':[[date, date, date], [1, 2, 3]],
                                'Propane':[[date, date, date], [.5, 1, 1.5]]}))

    Example with single date list supplied:
        res_nmhc_plot([date, date, date], {'ethane':[None, [1, 2, 3]],
                                'propane':[None, [.5, 1, 1.5]]})

    dates: list, of Python datetimes; if set, this applies to all compounds.
        If None, each compound supplies its own date values
    compound_dict: dict, {'compound_name':[dates, mrs]}
        keys: str, the name to be plotted and put into filename
        values: list, len(list) == 2, two parallel lists that are...
            dates: list, of Python datetimes. If None, dates come from dates input parameter (for all compounds)
            mrs: list, of [int/float/None]s; these are the mixing ratios to be plotted

    limits: dict, optional dictionary of limits including ['top','bottom','right','left']
    major_ticks: list, of major tick marks
    minor_ticks: list, of minor tick marks
    """

    import matplotlib.pyplot as plt
    from matplotlib.dates import DateFormatter

    f1 = plt.figure()
    ax = f1.gca()

    if dates is None: # dates supplied by individual compounds
        for compound, val_list in compound_dict.items():
            assert val_list[0] is not None, 'A supplied date list was None'
            assert len(val_list[0]) > 0 and len(val_list[0]) == len(val_list[1]), 'Supplied dates were empty or lengths did not match'
            ax.plot(val_list[0], val_list[1], '-o')

    else:
        for compound, val_list in compound_dict.items():
            ax.plot(dates, val_list[1], '-o')

    compounds_safe = []
    for k, _ in compound_dict.items():
        """Create a filename-safe list using the given legend items"""
        compounds_safe.append(k.replace('-', '_').replace('/', '_').lower())

    comp_list = ', '.join(compound_dict.keys())  # use real names for plot title
    fn_list = '_'.join(compounds_safe)  # use 'safe' names for filename

    if limits is not None:
        ax.set_xlim(right=limits.get('right'))
        ax.set_xlim(left=limits.get('left'))
        ax.set_ylim(top=limits.get('top'))
        ax.set_ylim(bottom=limits.get('bottom'))

    if major_ticks is not None:
        ax.set_xticks(major_ticks, minor=False)
    if minor_ticks is not None:
        ax.set_xticks(minor_ticks, minor=True)

    date_form = DateFormatter("%Y-%m-%d")
    ax.xaxis.set_major_formatter(date_form)

    [i.set_linewidth(2) for i in ax.spines.values()]
    ax.tick_params(axis='x', labelrotation=30)
    ax.tick_params(axis='both', which='major', size=8, width=2, labelsize=15)
    f1.set_size_inches(11.11, 7.406)

    ax.set_ylabel('Mixing Ratio (ppbv)', fontsize=20)
    ax.set_title(f'{comp_list}', fontsize=24, y= 1.02)
    ax.legend(compound_dict.keys())

    f1.subplots_adjust(bottom=.20)

    f1.savefig(f'{fn_list}_last_week.png', dpi=150)
    plt.close(f1)


def get_peak_data(run):
    """Useful for extracing all peak info from newly created GcRuns or Datums in service of integration corrections."""
    pas = [peak.pa for peak in run.peaks]
    rts = [peak.rt for peak in run.peaks]

    assert len(pas) == len(rts), "get_peak_data() produced lists of uneven lengths."

    return (pas, rts)


def check_c4_rts(run):
    """
    Acetylene and n-butane are not always caught by PeakSimple correctly, but several rules lead to much better
    integrations. Checking their retention times against i-butane is quite reliable. This takes one run at a time and
    if acetylene or n-butane do not match conditions, it finds the correct peaks (or at the very least un-labels
    incorrect matches).
    """

    if run is None:
        return None  # added so this can be passed a None during integrations w/o issue

    ibut_rt = run.get_rt('i-butane')
    nbut_rt = run.get_rt('n-butane')
    acet_rt = run.get_rt('acetylene')

    find_nbut = False
    find_acet = False  # default to both not needing to be found

    if ibut_rt is not None:
        if nbut_rt is not None:
            nbut_diff = nbut_rt - ibut_rt
            if .42< nbut_diff < .46:
                pass
            else:
                find_nbut = True  # needs to be found now
                pseudo_nbut = search_for_attr_value(run.peaks, 'name', 'n-butane')
                pseudo_nbut.name = '-'  # rename the imposter to null name

        if acet_rt is not None:
            acet_diff = acet_rt - ibut_rt
            if .3 < acet_diff < .4:
                pass
            else:
                pseudo_acet = search_for_attr_value(run.peaks, 'name', 'acetylene')
                pseudo_acet.name = '-'  # rename the imposter to null name
                find_acet = True  # needs to be found now

        if acet_rt is None or find_acet:
            acet_pool = [peak for peak in run.peaks if .3 < (peak.rt - ibut_rt) < .4]

            if len(acet_pool) == 0:
                pass
            else:
                acet = max(acet_pool, key=lambda peak: peak.pa)  # get largest peak in possible peaks
                acet.name = 'acetylene'

        if nbut_rt is None or find_nbut:
            nbut_pool = [peak for peak in run.peaks if .42< (peak.rt - ibut_rt) < .46]

            if len(nbut_pool) == 0:
                pass
            else:
                nbut = max(nbut_pool, key=lambda peak: peak.pa)  # get largest peak in possible peaks
                nbut.name = 'n-butane'

    return run



