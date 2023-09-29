#-----------------------------------------------------------------------------------
#
# ExcelGrabber.py
#
# Description:  Acquire HABS data from various data providers and insert into the
#               ESRI SDE.
#
#-----------------------------------------------------------------------------------

import sys, os, time, xmlrpclib, logging, socket, platform
from optparse import OptionParser
from ConfigParser import SafeConfigParser
import XLProc, textProc, ftpProc, SDEIO
from exception import InfobrokerError

#-------------------------------------------------------------------------------------

def getOptionsFromOptionParser():
    """
    Description:    Configure the option parser and process any command-line arguments.
    Input:          None.
    Output:         None.
    Return Value:   An OptionParser object.
    """
    usage = 'usage: %prog [options]'
    parser = OptionParser(usage)
    parser.add_option('-c', '--config',
                      dest='config_file',
                      default='/etc/ig-access/HABSGrabber.cfg',
                      action='store',
                      help='Path to configuration file [%default]')
    parser.add_option('-d',  '--dry-run',
                      dest='dry_run',
                      default=False,
                      action='store_true',
                      help='Dry run; do everything but write to the database [%default]')
    parser.add_option('-f', '--file',
                      dest='filename',
                      default=None,
                      action='store',
                      help='Name of spreadsheet file to process [%default]')
    parser.add_option('-g', '--debug',
                      dest='debug',
                      default=False,
                      action='store_true',
                      help='Run in debug mode (read: copious output) [%default]')
    parser.add_option('-r', '--row-number',
                      dest='startingRow',
                      type='int',
                      default=1,
                      action='store',
                      help='Starting row number in spreadsheet [%default]')
    parser.add_option('-x', '--exit-when-done',
                      dest='exitWhenDone',
                      default=False,
                      action='store_true',
                      help='Exit when data have been processed [%default]')
    parser.add_option('-l', '--last-file',
                      dest='use_last_file',
                      default=False,
                      action='store_true',
                      help='Process only the most recent file on the FTP server [%default]')
    parser.add_option('--ftp',
                      dest='ftp',
                      action='store_true',
                      default=False,
                      help='Process files from all known FTP servers (currently Florida and Texas) [%default]')
    parser.add_option('--florida-ftp',
                      dest='florida_ftp',
                      action='store_true',
                      default=False,
                      help='Process files from Florida\'s FTP server [%default]')
    parser.add_option('--texas-ftp',
                      dest='texas_ftp',
                      action='store_true',
                      default=False,
                      help='Process files from Texas\' FTP server [%default]')
    parser.add_option('--mississippi-ftp',
                      dest='mississippi_ftp',
                      action='store_true',
                      default=False,
                      help='Process files from Mississippi\'s FTP server [%default]')
    return parser.parse_args()

#-------------------------------------------------------------------------------------

def configureTheLogger(options):
    """
    Description:	Configure the logging module.
    """
    if options.debug:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.WARNING

    logging.basicConfig(level=logLevel,
                        format='%(asctime)-s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        stream=sys.stdout)


#-------------------------------------------------------------------------------------

def getInformationBroker(options, config):
    """
    Description:    Create an xmlrpclib ServerProxy instance and test the connection.
                    If this is not a dry run and the connection test fails, exit with
                    an error status.
    Input:          options: <obj>  An OptionParser instance.
                    config:  <obj>  A ConfigParser instance.
    Output:         Logging messages to stdout.
    Return Value:   An xmlrpclib ServerProxy instance.
    """
    
    infoBrokerInstance = config.get('globals','infoBrokerInstance')
    infoBroker = xmlrpclib.Server(infoBrokerInstance)
    #
    # If not a dry run, invoke a method on the 'Broker to test connectivity.
    # If it fails, log it and exit.
    #
    if not options.dry_run:
        logging.info('Connecting to Information Broker...')
        try:
            logging.info('  version %s'%infoBroker.version())
        except socket.error, msg:
            logging.fatal('Unable to connect to Information Broker: %s'%str(msg))
            sys.exit(-1)
            
    return infoBroker
    
#-------------------------------------------------------------------------------------

if __name__ == '__main__':
    #
    # Configure the option parser and process any command-line arguments.
    #
    options, args = getOptionsFromOptionParser()
    #
    # Configure the logger.
    #
    configureTheLogger(options)
    #
    # issue startup message
    #
    logging.warning('HABSGrabber starting up (pid=%d).'%os.getpid())
    #
    # write the process id to an external file 
    #
    #try:
    #    open('/var/run/habsgrabber.pid','w').write('%d'%os.getpid())
    #except IOError, msg:
    #    logging.warning('Unable to write PID to file: %s'%str(msg))
    logging.warning('Using config file %s.'%options.config_file)
    #
    # Read the configuration file.
    #
    config = SafeConfigParser()
    config.read(options.config_file)
    sleepInterval = int(config.get('globals','sleepInterval'))
    logging.info('DRY_RUN is %s.',`options.dry_run`)
    logging.info('USE_LAST_FILE is %s.',`options.use_last_file`)
    #
    # Connect to the InfoBroker.
    #
    infoBroker = getInformationBroker(options, config)
    #
    # Read ORGANISM records from the database and store the BIN_LEVEL records for each
    #
    try:
        organisms = SDEIO.getOrganisms(infoBroker, options)
        bin_levels = SDEIO.getBinLevels(infoBroker, organisms, options)
        #
        # If 'filename' is given, process it accordingly
        # NOTE: outside the FTP loop below; we don't want to process such files repeatedly
        #
        if options.filename is not None:
            filenameExtension =  os.path.splitext(options.filename)[1]
            if filenameExtension in ['.xls','.xlsx']:
                logging.warning('Using spreadsheet %s.'%options.filename)
                if os.path.split(options.filename)[1].startswith('TX'):
                    processor = XLProc.processSpreadsheetTX
                elif os.path.split(options.filename)[1].startswith('MS'):
                    processor = XLProc.processSpreadsheetMS
                elif os.path.split(options.filename)[1].startswith('FL'):
                    processor = XLProc.processSpreadsheetFL
                args = (options.filename, organisms, bin_levels, options, config)
            elif filenameExtension == '.txt':
                logging.warning('Using text file %s.'%options.filename)
                if os.path.split(options.filename)[1].startswith('TX'):
                    processor = textProc.processTextTX
                elif os.path.split(options.filename)[1].startswith('FL'):
                    processor = textProc.processTextFL
                elif os.path.split(options.filename)[1].startswith('AL'):
                    processor = textProc.processTextAL
                elif os.path.split(options.filename)[1].startswith('MS'):
                    processor = textProc.processTextMS
                args = (options.filename, organisms, bin_levels, options, config, infoBroker)
            #
            # Invoke the processor
            #
            SDEIO.processHABSData(infoBroker, processor, args, options)

    except (socket.error, InfobrokerError, xmlrpclib.ProtocolError), msg:
        logging.critical('Information Broker error: %s'%str(msg))
    #
    # Build a quick dictionary based on FTP options entered at the command line.
    #
    ftpProcessors = {}
    if options.ftp:
        ftpProcessors.update({'Texas':ftpProc.processTexasFTP,
                              'Florida': ftpProc.processFloridaFTP,
                              'Mississippi': ftpProc.processMississippiFTP})
    else:
        if options.florida_ftp:
            ftpProcessors.update({'Florida': ftpProc.processFloridaFTP})
        if options.texas_ftp:
            ftpProcessors.update({'Texas': ftpProc.processTexasFTP})
        if options.mississippi_ftp:
            ftpProcessors.update({'Mississippi': ftpProc.processMississippiFTP})
    
    if ftpProcessors:
        #
        # Repeat until spanked
        #
        while True:
            try:
                #
                # Re-read ORGANISMs and BIN_LEVELs before repeating the loop
                #
                organisms = SDEIO.getOrganisms(infoBroker, options)
                bin_levels = SDEIO.getBinLevels(infoBroker, organisms, options)
                args = (organisms, bin_levels, options, config, infoBroker)
                for processorName, processorFunc in ftpProcessors.items():
                    logging.info('Checking FTP for %s data.'%processorName)
                    SDEIO.processHABSData(infoBroker, processorFunc, args, options)
            except (socket.error, InfobrokerError, xmlrpclib.ProtocolError), msg:
                logging.critical('Information Broker error: %s'%str(msg))
            #
            # Break out of the loop if 'exitWhenDone' is set
            # Otherwise sleep for 'sleepInterval' seconds and repeat
            #
            if options.exitWhenDone:
                break
            else:
                logging.info('Sleeping for %d seconds.'%sleepInterval)
                time.sleep(sleepInterval)

    logging.warning('Done.')
