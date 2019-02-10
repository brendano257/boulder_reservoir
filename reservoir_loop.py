import os
import asyncio

homedir = os.getcwd()
locallogdir = 'log'
plotdir = 'plots'


def print_now(string):
    """Takes a string and prints it, but appended with - {ISO Datetime of the time it was printed}"""
    from datetime import datetime
    print(f"{string} - {datetime.now().isoformat(' ')}")


async def check_load_logs(logpath, homedir, sleeptime):
    '''
    Checks the directory against the database for new log files. Loads and commits
    to db if there are any new files.

    Basic format: Connect to the db, check for new log files. If new files
    exist, load and commit them to the db. In all cases, sleep for 30s before
    looping back.
    '''

    while True:
        from reservoir_nmhc import connect_to_reservoir_db, TempDir, LogFile, fix_off_dates, read_log_file

        engine, session, Base = connect_to_reservoir_db('sqlite:///reservoir.sqlite', homedir)
        Base.metadata.create_all(engine)

        LogFiles = session.query(LogFile).order_by(LogFile.id).all()  # list of all log objects

        with os.scandir(logpath) as files:
            logfns = [file.name for file in files if 'l.txt' in file.name]

        if len(logfns) is 0:
            await asyncio.sleep(sleeptime)
            print('There we no log files in the directory!')
            continue  # no logs in directory? Sleep and look again

        logs_in_db = [log.filename for log in LogFiles]  # log names

        logs_to_load = []
        for log in logfns:
            if log not in logs_in_db:
                logs_to_load.append(log)  # add files if not in the database filenames

        if len(logs_to_load) is 0:
            print('No new logs were found.')
            await asyncio.sleep(sleeptime)
            session.commit()
            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)
        else:
            new_logs = []
            with TempDir(logpath):
                for log in logs_to_load:
                    new_logs.append(read_log_file(log))

            if len(new_logs) != 0:
                fix_off_dates(new_logs, [])

                for item in new_logs:
                    session.merge(item)
                print('New logs were added!')

            session.commit()
            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)


async def check_load_pas(filename, directory, sleeptime):
    """
    Basic format: Checks the file size of the PA log, opens it if it's bigger
    than before, and reads from the last recorded line onwards. Any new lines
    are added as objects and committed. All exits sleep for 30s before re-upping.
    """
    pa_file_size = 0  # always assume all lines could be new when initialized
    start_line = 0  # defaults set for testing, not runtime

    while True:
        from reservoir_nmhc import connect_to_reservoir_db, TempDir, NmhcLine, fix_off_dates, read_pa_line

        engine, session, Base = connect_to_reservoir_db('sqlite:///reservoir.sqlite', directory)
        Base.metadata.create_all(engine)

        NmhcLines = session.query(NmhcLine).order_by(NmhcLine.id).all()
        line_dates = [line.date for line in NmhcLines]

        from pathlib import Path

        pa_path = Path(directory)/filename

        if os.path.isfile(pa_path):
            with TempDir(directory):
                new_file_size = os.path.getsize(filename)

            if new_file_size > pa_file_size:
                with TempDir(directory):
                    contents = open('NMHC_PA.LOG').readlines()

                new_lines = []
                for line in contents[start_line:]:
                    try:
                        with TempDir(directory):
                            new_lines.append(read_pa_line(line))
                    except:
                        print('A line in NMHC_PA.LOG was not processed by read_pa_line() due to an exception.')
                        print(f'The line was: {line}')

                fix_off_dates([], new_lines)  # correct dates for lines if necessary

                if len(new_lines) is 0:
                    print('No new pa lines added.')
                    await asyncio.sleep(sleeptime)
                    continue

                for item in new_lines:
                    if item.date not in line_dates: #prevents duplicates in db
                        line_dates.append(item.date) #prevents duplicates in one load
                        session.merge(item)

                session.commit()

                start_line = len(contents)
                pa_file_size = new_file_size # set filesize to current file size
                print('Some PA lines found and added.')
                await asyncio.sleep(sleeptime)

            else:
                print('PA file was the same size, so it was not touched.')
                await asyncio.sleep(sleeptime)

        else:
            print('PA file did not exist!')
            print('PA file did not exist!')
            await asyncio.sleep(sleeptime)

        await asyncio.sleep(sleeptime)
        session.close()
        engine.dispose()


async def create_gc_runs(directory, sleeptime):

    while True:
        print('Running create_gc_runs()')
        from reservoir_nmhc import LogFile, NmhcLine, GcRun
        from reservoir_nmhc import connect_to_reservoir_db

        engine, session, Base = connect_to_reservoir_db('sqlite:///reservoir.sqlite', directory)
        Base.metadata.create_all(engine)

        NmhcLines = (session.query(NmhcLine)
                    .filter(NmhcLine.status == 'single')
                    .order_by(NmhcLine.id).all())

        LogFiles = (session.query(LogFile)
                    .filter(LogFile.status == 'single')
                    .order_by(LogFile.id).all())

        GcRuns = session.query(GcRun).order_by(GcRun.id).all()
        run_dates = [run.date_end for run in GcRuns]

        from reservoir_nmhc import match_log_to_pa

        GcRuns = match_log_to_pa(LogFiles, NmhcLines)

        for run in GcRuns:
            if run.date_end not in run_dates:
                run_dates.append(run.date_end)

                from reservoir_nmhc import check_c4_rts
                run = check_c4_rts(run)  # make any possible acetylene/nbutane corrections

                session.merge(run)
        session.commit()

        session.close()
        engine.dispose()
        await asyncio.sleep(sleeptime)


async def load_crfs(directory, sleeptime):

    while True:
        print('Running load_crfs()')
        from reservoir_nmhc import read_crf_data, Crf, connect_to_reservoir_db, TempDir

        engine, session, Base = connect_to_reservoir_db('sqlite:///reservoir.sqlite', directory)
        Base.metadata.create_all(engine)

        with TempDir(homedir):
            Crfs = read_crf_data('reservoir_CRFs.txt')

        Crfs_in_db = session.query(Crf).order_by(Crf.id).all()
        crf_dates = [rf.date_start for rf in Crfs_in_db]

        for rf in Crfs:
            if rf.date_start not in crf_dates: # prevent duplicates in db
                crf_dates.append(rf.date_start) # prevent duplicates in this load
                session.merge(rf)

        session.commit()

        session.close()
        engine.dispose()
        await asyncio.sleep(sleeptime)


async def integrate_runs(directory, sleeptime):
    from datetime import datetime

    while True:
        print('Running integrate_runs()')
        from reservoir_nmhc import find_crf
        from reservoir_nmhc import GcRun, Datum, Crf

        from reservoir_nmhc import connect_to_reservoir_db

        engine, session, Base = connect_to_reservoir_db('sqlite:///reservoir.sqlite', directory)
        Base.metadata.create_all(engine)

        GcRuns = (session.query(GcRun)
                .filter(GcRun.data_id == None)
                .order_by(GcRun.id).all()) # get all un-integrated runs

        Crfs = session.query(Crf).order_by(Crf.id).all() # get all crfs

        data = [] # Match all runs with available CRFs
        for run in GcRuns:
            run.crfs = find_crf(Crfs, run.date_end)
            session.commit() # commit changes to crfs?
            data.append(run.integrate())

        data_in_db = session.query(Datum).order_by(Datum.id).all()
        data_dates = [d.date_end for d in data_in_db]

        if len(data) is 0:
            print(f'No data to integrate found at {datetime.now()}')
            session.commit()
            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)

        else:
            for datum in data:
                if datum is not None and datum.date_end not in data_dates: # prevent duplicates in db
                    data_dates.append(datum.date_end) # prevent duplicates on this load
                    session.merge(datum)
                    print(f'Data {datum} was added!')

            session.commit()

            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)


async def plot_new_data(directory, plotdir, sleeptime):
    """
    Date limits have been tinkered with to correctly plot provided data.
    """

    days_to_plot = 3

    while True:
        print('Running plot_new_data()')
        data_len = 0
        from reservoir_nmhc import connect_to_reservoir_db, TempDir, get_dates_mrs, res_nmhc_plot
        from datetime import datetime
        import datetime as dt

        engine, session, Base = connect_to_reservoir_db('sqlite:///reservoir.sqlite', directory)
        Base.metadata.create_all(engine)

        # now = datetime.now()  # save 'now' as the start of making plots
        # date_ago = now - dt.timedelta(days=days_to_plot+1)  # set a static limit for retrieving data at beginning of plot cycle
        date_ago = datetime(2019,1,15)  #fake value for provided data

        date_limits = dict()
        date_limits['right'] = datetime(2019, 1, 28).replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=1)  # end of last day
        date_limits['left'] = date_limits['right'] - dt.timedelta(days=days_to_plot)

        ## For use at runtime:
        # date_limits['right'] = now.replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=1)  # end of last day
        # date_limits['left'] = date_limits['right'] - dt.timedelta(days=days_to_plot)

        major_ticks = [date_limits['right'] - dt.timedelta(days=x) for x in range(0, days_to_plot+1)]  # make dynamic ticks
        minor_ticks = [date_limits['right'] - dt.timedelta(hours=x*6) for x in range(0, days_to_plot*4+1)]

        try:
            _ , dates = get_dates_mrs(session, 'ethane', date_start=date_ago)  # get dates for data length

        except ValueError:
            print('No new data was found. Plots were not created.')
            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)
            continue

        if len(dates) != data_len:
            with TempDir(plotdir): ## PLOT ethane and propane
                ethane_mrs, ethane_dates = get_dates_mrs(session, 'ethane', date_start=date_ago)
                propane_mrs, propane_dates = get_dates_mrs(session, 'propane', date_start=date_ago)
                res_nmhc_plot(None, ({'Ethane': [ethane_dates, ethane_mrs],
                                      'Propane': [propane_dates, propane_mrs]}),
                              limits={'right': date_limits.get('right',None),
                                      'left': date_limits.get('left', None),
                                      'bottom': 0},
                              major_ticks=major_ticks,
                              minor_ticks=minor_ticks)

            with TempDir(plotdir): ## PLOT i-butane, n-butane, acetylene
                ibut_mrs, ibut_dates = get_dates_mrs(session, 'i-butane', date_start=date_ago)
                nbut_mrs, nbut_dates = get_dates_mrs(session, 'n-butane', date_start=date_ago)
                acet_mrs, acet_dates = get_dates_mrs(session, 'acetylene', date_start=date_ago)

                res_nmhc_plot(None, ({'i-Butane': [ibut_dates, ibut_mrs],
                                      'n-Butane': [nbut_dates, nbut_mrs],
                                      'Acetylene': [acet_dates, acet_mrs]}),
                              limits={'right': date_limits.get('right',None),
                                      'left': date_limits.get('left', None),
                                      'bottom': 0},
                              major_ticks=major_ticks,
                              minor_ticks=minor_ticks)

            with TempDir(plotdir): ## PLOT i-pentane and n-pentane, & ratio
                ipent_mrs, ipent_dates = get_dates_mrs(session, 'i-pentane', date_start=date_ago)
                npent_mrs, npent_dates = get_dates_mrs(session, 'n-pentane', date_start=date_ago)

                inpent_ratio = []

                for i, n in zip(ipent_mrs, npent_mrs):
                    if n == 0 or n == None:
                        inpent_ratio.append(None)
                    elif i == None:
                        inpent_ratio.append(None)
                    else:
                        inpent_ratio.append(i/n)

                res_nmhc_plot(dates, ({'i-Pentane': [ipent_dates, ipent_mrs],
                                       'n-Pentane': [npent_dates, npent_mrs]}),
                              limits={'right': date_limits.get('right',None),
                                      'left': date_limits.get('left', None),
                                      'bottom': 0},
                              major_ticks=major_ticks,
                              minor_ticks=minor_ticks)

                res_nmhc_plot(None, ({'i/n Pentane ratio': [ipent_dates, inpent_ratio]}),
                              limits={'right': date_limits.get('right',None),
                                      'left': date_limits.get('left', None),
                                      'bottom': 0,
                                      'top': 3},
                              major_ticks=major_ticks,
                              minor_ticks=minor_ticks)

            with TempDir(plotdir): ## PLOT benzene and toluene
                benz_mrs, benz_dates = get_dates_mrs(session, 'benzene', date_start=date_ago)
                tol_mrs, tol_dates = get_dates_mrs(session, 'toluene', date_start=date_ago)

                res_nmhc_plot(None, ({'Benzene': [benz_dates, benz_mrs],
                                      'Toluene': [tol_dates, tol_mrs]}),
                              limits={'right': date_limits.get('right',None),
                                      'left': date_limits.get('left', None),
                                      'bottom': 0},
                              major_ticks=major_ticks,
                              minor_ticks=minor_ticks)

            print('New data plots created!')

            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)
        else:
            print('New data plots were not created, there was no new data.')

            session.close()
            engine.dispose()
            await asyncio.sleep(sleeptime)


os.chdir(homedir)

loop = asyncio.get_event_loop()

loop.create_task(check_load_logs(locallogdir, homedir, 5))
loop.create_task(check_load_pas('NMHC_PA.LOG', homedir, 5))
loop.create_task(create_gc_runs(homedir, 5))
loop.create_task(load_crfs(homedir, 5))
loop.create_task(integrate_runs(homedir, 5))
loop.create_task(plot_new_data(homedir, plotdir, 5))

loop.run_forever()
