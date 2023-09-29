#---------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------

import logging, os, re
from ftp import ftpConnect, ftpDownloadText, ftpDownloadBinary, ftpDisconnect, ftpGetFileNames
import ftplib
MAX_RETRIES = 10

#---------------------------------------------------------------------------------------------

def processFloridaFTP(organisms, bin_levels, options, config, infoBroker):
    """ 
Description:	Ingest Florida HABS text files from their FTP server.
Input:		organisms:	List of ORGANISM records from the database.
		options:	Command-line arguments (via OptionParser)
		bin_levels:	List of BIN_LEVEL records from the database.
                config:         ConfigParser object.
                infoBroker:     xmlrpclib.Server object.
Return Value:	This method is a generator.  For each line processed from the text file,
		this method yields a 3-Tuple of Dictionaries containing rowdata, 
		quality-assurance values, and quality-assurance comments.
Discussion:	The text files are similar in form to the spreadsheets (ref.  XLProc.processSpreadsheetFL()
                but with some important differences:
		*  The contain one year's worth of data in a "sliding window", so that
		   data are repeated in the text file until they fall out of the rolling 
		   time frame.  This means we must keep track of the (thankfully) unique
		   sample id so we don't repeatedly ingest and store the same data. We also
		   must keep track of which files have already been processed.
		*  Only data for K. brevis are reported.
    """

    from textProc import processTextFL

    dataFileDir = config.get('globals','dataFileDir')
    ftpFileDir = config.get('globals','ftpFileDir')
    lastFileIdFilename = '%s/FL_processedFiles.txt'%dataFileDir 
    #
    # List of files already processed
    #
    try:
        filesAlreadyProcessed = [x.strip() for x in open(lastFileIdFilename,'r').read().split('\n')]
    except IOError:
        filesAlreadyProcessed = []
    #
    # Folder to store downloaded text files
    #
    if not os.path.exists(ftpFileDir):
        os.mkdir(ftpFileDir)
    #
    # Folder to store private data files
    #
    if not os.path.exists(dataFileDir):
        os.mkdir(dataFileDir)
    #
    # Connect to the FTP server and get the file listing.
    #
    ftpError = True
    loopCtr = 0
    filesToProcess = []
    while ftpError and loopCtr < MAX_RETRIES:
        loopCtr += 1
        try:
           ftpSvr = ftpConnect(config.get('florida-ftp','host'),
                               config.get('florida-ftp','user'),
                               config.get('florida-ftp','pw'),
                               config.get('florida-ftp','dir'))
           filePattern = re.compile(r'HABsos_water')

           for fileName in ftpGetFileNames(ftpSvr, 
                                           filePattern, 
                                           filesAlreadyProcessed, 
                                           options.use_last_file):
               ftpDownloadText(ftpSvr, fileName, config)
               filesToProcess.append(fileName)
           ftpDisconnect(ftpSvr)
           ftpError = False

           for fileName in sorted(list(set(filesToProcess))):
               for rowdata, qadata, qacomments in processTextFL('%s/%s'%(ftpFileDir, fileName),
                                                                organisms,
                                                                bin_levels,
                                                                options,
                                                                config,
                                                                infoBroker):
                   yield rowdata, qadata, qacomments
               yield 'FLUSH','FLUSH','FLUSH'
                       
               filesAlreadyProcessed.append(fileName)

               if not options.dry_run:
                   try:
                       logging.info('Storing last file processed (%s).'%fileName)
                       open(lastFileIdFilename, 'a').write('%s\n'%fileName)
                   except UnboundLocalError:
                       pass

        except ftplib.all_errors, msg:
            nRetries = MAX_RETRIES - loopCtr
            if nRetries > 0:
                logging.warning('FTP error encountered: %s.  Will retry %d times...'%(str(msg), nRetries))
            else:
                logging.warning('FTP error encountered: %s.  Giving up.'%str(msg))

#---------------------------------------------------------------------------------------------

def processTexasFTP(organisms, bin_levels, options, config, infoBroker):
    """ 
Description:	Ingest Texas HABS spreadsheet files from their FTP server.
Input:		organisms:	List of ORGANISM records from the database.
		options:	Command-line arguments (via OptionParser)
		bin_levels:	List of BIN_LEVEL records from the database.
                config:         ConfigParser object.
                infoBroker:     xmlrpclib.Server object.   (unused in this method)
Return Value:	This method is a generator.  For each line processed from the text file,
		this method yields a 3-Tuple of Dictionaries containing rowdata, 
		quality-assurance values, and quality-assurance comments.
    """

    from XLProc import processSpreadsheetTX

    dataFileDir = config.get('globals','dataFileDir')
    ftpFileDir = config.get('globals','ftpFileDir')
    #
    # List of files already processed
    #
    try:
        filesAlreadyProcessed = [x.strip() for x in open('%s/TX_processedFiles.txt'%dataFileDir,'r').read().split('\n')]
    except IOError:
        filesAlreadyProcessed = []
    #
    # Folder to store downloaded files
    #
    if not os.path.exists(ftpFileDir):
        os.mkdir(ftpFileDir)
    #
    # Folder to store private data files
    #
    if not os.path.exists(dataFileDir):
        os.mkdir(dataFileDir)
    #
    # Connect to the FTP server and get the file listing.
    #
    ftpError = True
    loopCtr = 0
    filesToProcess = []
    while ftpError and loopCtr < MAX_RETRIES:
        loopCtr += 1
        try:
            ftpSvr = ftpConnect(config.get('texas-ftp','host'),
                                config.get('texas-ftp','user'),
                                config.get('texas-ftp','pw'),
                                config.get('texas-ftp','dir'))
            filePattern = re.compile('.*')
            for fileName in ftpGetFileNames(fptSvr, 
                                            filePattern, 
                                            filesAlreadyProcessed):
                ftpDownloadBinary(ftpSvr, fileName, config)
                filesToProcess.append(fileName)
            ftpError = False
            ftpDisconnect(ftpSvr)
            
            for fileName in sorted(list(set(filesToProcess))):
                for rowdata, qadata, qacomments in processSpreadsheetTX('%s/%s'%(ftpFileDir, fileName),
                                                                        organisms,
                                                                        bin_levels,
                                                                        options,
                                                                        config):
                    yield rowdata, qadata, qacomments
                yield 'FLUSH','FLUSH','FLUSH'
                            
                filesAlreadyProcessed.append(fileName)

                if not options.dry_run:
                    try:
                        logging.info('Storing last file processed (%s).'%fileName)
                        open('%s/TX_processedFiles.txt'%dataFileDir,'a').write('%s\n'%fileName)
                    except UnboundLocalError:
                        pass

        except ftplib.all_errors, msg:
            nRetries = MAX_RETRIES - loopCtr
            if nRetries > 0:
                logging.warning('FTP error encountered: %s.  Will retry %d times...'%(str(msg), nRetries))
            else:
                logging.warning('FTP error encountered: %s.  Giving up.'%str(msg))

#---------------------------------------------------------------------------------------------

def processMississippiFTP(organisms, bin_levels, options, config, infoBroker):
    """ 
Description:	Ingest Mississippi HABS spreadsheet files from their FTP server.
Input:		organisms:	List of ORGANISM records from the database.
		options:	Command-line arguments (via OptionParser)
		bin_levels:	List of BIN_LEVEL records from the database.
                config:         ConfigParser object.
                infoBroker:     xmlrpclib.Server object.   (unused in this method)
Return Value:	This method is a generator.  For each line processed from the text file,
		this method yields a 3-Tuple of Dictionaries containing rowdata, 
		quality-assurance values, and quality-assurance comments.
    """

    from XLProc import processSpreadsheetMS

    dataFileDir = config.get('globals','dataFileDir')
    ftpFileDir = config.get('globals','ftpFileDir')
    #
    # List of files already processed
    #
    try:
        filesAlreadyProcessed = [x.strip() for x in open('%s/MS_processedFiles.txt'%dataFileDir,'r').read().split('\n')]
    except IOError:
        filesAlreadyProcessed = []
    #
    # Folder to store downloaded files
    #
    if not os.path.exists(ftpFileDir):
        os.mkdir(ftpFileDir)
    #
    # Folder to store private data files
    #
    if not os.path.exists(dataFileDir):
        os.mkdir(dataFileDir)
    #
    # Connect to the FTP server and get the file listing.
    #
    ftpError = True
    loopCtr = 0
    filesToProcess = []
    while ftpError and loopCtr < MAX_RETRIES:
        loopCtr += 1
        try:
            ftpSvr = ftpConnect(config.get('mississippi-ftp','host'),
                                config.get('mississippi-ftp','user'),
                                config.get('mississippi-ftp','pw'),
                                config.get('mississippi-ftp','dir'))

            for fileName in ftpGetFileNames(fptSvr, 
                                            filePattern, 
                                            filesAlreadyProcessed):
                ftpDownloadBinary(ftpSvr, fileName, config)
                filesToProcess.append(fileName)
            ftpError = False
            ftpDisconnect(ftpSvr)


            for fileName in sorted(list(set(filesToProcess))):
                for rowdata, qadata, qacomments in processSpreadsheetMS('%s/%s'%(ftpFileDir, fileName),
                                                                        organisms,
                                                                        bin_levels,
                                                                        options,
                                                                        config):
                    yield rowdata, qadata, qacomments
                yield 'FLUSH','FLUSH','FLUSH'
                            
                filesAlreadyProcessed.append(fileName)

                if not options.dry_run:
                    try:
                        logging.info('Storing last file processed (%s).'%fileName)
                        open('%s/MS_processedFiles.txt'%dataFileDir,'a').write('%s\n'%fileName)
                    except UnboundLocalError:
                        pass

        except ftplib.all_errors, msg:
            nRetries = MAX_RETRIES - loopCtr
            if nRetries > 0:
                logging.warning('FTP error encountered: %s.  Will retry %d times...'%(str(msg), nRetries))
            else:
                logging.warning('FTP error encountered: %s.  Giving up.'%str(msg))
