#---------------------------------------------------------------------------------------------
#
# ftp.py
#    FTP-related functions for the HABSGrabber application.
#    FTP exceptions will be handled by the caller
#
#---------------------------------------------------------------------------------------------

import os,ftplib, logging, gzip

lines = []
fp = None

#---------------------------------------------------------------------------------------------

def ftpConnect(ftpHost, ftpUser, ftpPassword, ftpDir=None):
    """
    Description:    Connect to an FTP server.
    Input:          ftpHost:      <string>  name of the FTP server.
                    ftpUser:      <string>  name of a user account on the server.
                    ftpPassword:  <string>  user's password on the server.
                    ftpDir:       <string)  (optional) target directory on the server.
    Return Value:   ftpSvr:       <obj>     ftplib.FTP object.
    """

    logging.info('Connecting to %s.'%ftpHost)
    ftpSvr = ftplib.FTP(ftpHost, ftpUser, ftpPassword, timeout=30)
    if ftpDir is not None:
        logging.info('Changing directory to %s.'%`ftpDir`)
        ftpSvr.cwd(ftpDir)
    return ftpSvr

#---------------------------------------------------------------------------------------------

def ftpDisconnect(ftpSvr):
    """
    Description:    Disconnect from an FTP server.
    Input:          ftpSvr:  <obj>  ftplib.FTP object.
    Return Value:   None.
    """

    logging.info('Disconnecting from %s.'%ftpSvr.host)
    ftpSvr.quit()
    
#---------------------------------------------------------------------------------------------

def ftp_retrlines_callback(line):
    """
    Description:    Callback method for ftplib.retrlines() below.
    Input:          line   <string>  One line from the file being downloaded.
    Output:         None.
    Return Value:   None.  Lines from the file are stored in the global varible 'lines'.
    """

    global lines

    if len(line.strip()) > 1:
        lines.append(line)

#---------------------------------------------------------------------------------------------

def ftpDownloadText(ftpSvr, fileName, config, compress=True):
    """
    Description:    Download a text file from an FTP server.  Also store the downloaded files
                    locally and (optionally) compress them with gzip.
    Input:          ftpSvr:    <obj>    ftplib FTP object.
                    fileName:  <string> name of the file to download.
                    compress:  <bool>   compress the file with gzip.
    Output:         The contents of the downloaded file will be stored in 'ftpFileDir'.
    Return Value:   None.
    """

    global lines

    ftpFileDir = config.get('globals','ftpFileDir')
    lines = []

    logging.warning('Getting text file %s.'%`fileName`)
    ftpSvr.retrlines('RETR %s'%fileName, ftp_retrlines_callback)
    open('%s/%s'%(ftpFileDir, fileName), 'w').write('\n'.join(lines))
    logging.info('...OK')

    """
    # 20130403 DES Not sure if this part is necessary
    if compress:
        #
        # Compress the file we just downloaded on disk
        #
        logging.info('Compressing file on local disk.')
        ftpFileName = '%s/%s.gz'%(ftpFileDir, fileName)
        gzfp = gzip.open(ftpFileName, 'wb')
        gzfp.write('\n'.join(lines))
        gzfp.close()
        logging.info('...OK')

    return lines
    """

#---------------------------------------------------------------------------------------------

def ftpDownloadBinary(ftpSvr, fileName, config, compress=True):
    """
    Description:    Download a binary file from an FTP server.  
    Input:          ftpSvr:    <obj>    ftplib FTP object.
                    fileName:  <string> name of the file to download.
                    compress:  <bool>   Compress the file using gzip after download.
    Return Value:   Contents of the file, with each line stored as a List element.
                    Lines from the file are stored in memory via ftp_retrlines_callback()
    """

    ftpFileDir = config.get('globals','ftpFileDir')

    logging.warning('Getting binary file %s.'%`fileName`)
    ftpSvr.retrbinary('RETR %s'%fileName, open('%s/%s'%(ftpFileDir, fileName), 'wb').write)
    logging.info('OK')

    """
    # DES 20130403 Not sure if this part is necessary
    retval = open('%s/%s'%(ftpFileDir, fileName), 'rb').read()
    logging.info('...OK')
    if compress:
        #
        # Compress the file we just downloaded on disk
        #
        logging.info('  Compressing file on local disk.')
        ftpFileName = '%s/%s.gz'%(ftpFileDir, fileName)
        gzfp = gzip.open(ftpFileName, 'wb')
        gzfp.write('\n'.join(lines))
        gzfp.close()
        logging.info('  ...OK')
    return retval
    """

#---------------------------------------------------------------------------------------------

def ftpGetFileNames(ftpSvr, filePattern, filesAlreadyProcessed=[], useLastFile=False):
    """
    Description:    Get a list of file names from the FTP server.
    Input:          ftpSvr:                 <obj>    ftplib FTP object.
                    filePattern:            <obj>    regex object for file matching.
                    filesAlreadyProcessed:  <list>   list of files already processed.
                    useLastFile:            <bool>   if True, only return the last file in the list
                                                     (presumed to be the most recent).
    Return Value:   This method is a generator.  Return value will be 0 or more filename strings.
                    
    """

    fileNames = ftpSvr.nlst()
    #
    # Filter out files that have not already been processed
    #
    fileNames = sorted([x for x in fileNames if filePattern.match(x) and x not in filesAlreadyProcessed])

    if fileNames:

        if useLastFile:
            fileNames = [fileNames[-1]]
            logging.info('Most recent file is %s'%fileNames[-1])

        for x in fileNames:
            yield x
    else:
        logging.info('There are no new files to be processed.')
