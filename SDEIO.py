#-----------------------------------------------------------------------------------
#
# SDEIO.py
#    Methods to interact with the SDE database via the NCDDC Information Broker.
#
#-----------------------------------------------------------------------------------

import logging, time, types, pprint
from regexes import token_regex
from statics import staticOrganisms, staticBinLevels
from constants import *
from exception import InfobrokerError

contacts = []
contact_ids = []
contact_ctr = 1
sample_sites = []
samplesite_ids = []
samplesite_ctr = 1

#-----------------------------------------------------------------------------------

def flush(packages, infoBroker, options):
    """ """

    logging.info('flush()')
    logging.info('  %d packages'%len(packages))
    if not options.dry_run and packages:
        packagesToInsert = []
        packagesToUpdate = []
        rowIdsToUpdate = []
        for package in packages:
            try:
                #
                # Packages from Florida will contain 'OPERATION' keys
                # due to their use of "proof dates", meaning that
                # some records may require updating instead of inserting
                #
                if package['OPERATION'] == 'insert':
                    packagesToInsert.append(package)
                else:
                    #
                    # Query the database to get the objectid of this record
                    # so we can update it
                    #
                    r = infoBroker.querySDE(package['sde_instance'],
                                            package['sde_table'],
                                            'OBJECTID',
                                            {'habsos_id':package['habsos_id']})
                    if 'Error' in r:
                        raise InfobrokerError(r['Error'])
                    else:
                        try:
                            rowIdsToUpdate.append(r[0]['OBJECTID'])
                            packagesToUpdate.append(package)
                        except IndexError:
                            pass
            except KeyError:
                packagesToInsert.append(package)

        if packagesToInsert:
            logging.warning('  Inserting %d packages'%len(packagesToInsert))
            r = infoBroker.writeToSDE(packagesToInsert)
            for item in r:
                if 'Error' in item:
                    raise InfobrokerError(item['Error'])
            logging.info(`r`)

        if packagesToUpdate:
            logging.warning('  Updating %d packages'%len(packagesToUpdate))
            r = infoBroker.updateSDE(rowIdsToUpdate, packagesToUpdate)
            for item in r:
                if 'Error' in item:
                    raise InfobrokerError(item['Error'])
            logging.info(`r`)
  
#-----------------------------------------------------------------------------------

def getBinLevels(infoBroker, organisms, options):
    """ 
    Description:	Retrieve bin level data from the database and organize by organism name.
    Input:		infoBroker:	XMLRPC ServerProxy instance pointing to the Information Broker.
			organisms:	List of ORGANISM records from the database.
			options:	OptionParser command-line options.
    Return Value:	Dictionary of bin level data, keyed by organism name.
    """

    bin_levels = {}
    logging.info('Organisms in database:')
    for organism in organisms:
        organismName = '%s %s'%(organism['GENUS'],organism['SPECIES'])
        organism['ORGANISM_NAME'] = organismName
        logging.info('   %s'%organismName)
        if not options.dry_run:
            vals = infoBroker.querySDE('habsos','BIN_LEVELS',[],{'ORGANISM_ID':organism['OBJECTID']})
            if 'Error' in vals:
                raise InfobrokerError(vals['Error'])
            else:
                bin_levels[organismName] = vals 
        else:
            bin_levels[organismName] = [x for x in staticBinLevels if x['ORGANISM_ID'] == organism['OBJECTID']]

    return bin_levels

#-----------------------------------------------------------------------------------

def getOrganisms(infoBroker, options):
    """ 
    Description:	Retrieve organism data from the database.
    Input:		infoBroker:	XMLRPC ServerProxy instance pointing to the Information Broker.
			options:	OptionParser command-line options.
    Return Value:	List of organism records from the database.
    """

    organisms = []

    if not options.dry_run:
        organisms = infoBroker.querySDE('habsos','ORGANISM')
        if 'Error' in organisms:
            raise InfobrokerError(organisms['Error'])
    else:
        organisms = staticOrganisms

    return organisms

#-----------------------------------------------------------------------------------

def getContactId(infoBroker, rowdata, options):
    """
    Description:    Return the primary key value of a contact record in the database.
    Input:	    infoBroker:	XMLRPC ServerProxy instance pointing to the Information Broker.
                    rowdata:  <dict>  A data record retrieved from the data source.
                    options:  <obj>   OptionParser command-line options.
    Return Value:   The primary key value of a contact record.
    Discussion:     The algorithm relies on the presence of certain key-value pairs in the 'rowdata'
                    parameter.  If none are present, no contact id will be returned.  If they are
                    present, the algorithm will first see if the record is stored in the local cache.
                    If not, the algorithm will query the database.  If not in the database, the algorithm
                    will insert the record.  The algorithm will store the contact record in the local
                    cache to avoid excessive queries on the database.
    """

    global contacts, contact_ids, contact_ctr

    contact = {}
    contact_id = None

    try:
        contact.update(agencyMap[rowdata['AGENCY']])
    except KeyError, msg:
        pass

    try:
        cnames = rowdata['COLLECTOR'].split(',')
        for name in cnames:
            try:
                first, last = token_regex.findall(name)
                contact.update({'fname':first,
                                'lname':last})
            except ValueError:
                pass
    except KeyError:
        pass

    if contact:
        logging.info('    Contact Info')
        for k in contact:
            logging.info('      %s: %s'%(k, contact[k]))
        #
        # First see if the contact info is stored locally.
        #
        if contact in contacts:
            contact_id = contact_ids[contacts.index(contact)]
            logging.info('    Got contact info from list (id=%d).'%contact_id)
        else:
            #
            # Store the contact info in the local list, then query the database to see if it's there.
            # If it's not in the database, store it there.
            #
            contacts.append(contact)
            if not options.dry_run:
                try:
                    contact_id = infoBroker.querySDE('habsos',
                                                     'HABSOS_CONTACTS',
                                                     'OBJECTID',
                                                     contact)
                    if 'Error' in contact_id:
                        raise InfobrokerError(contact_id['Error'])
                    else:
                        contact_id = contact_id[0]['OBJECTID']
                    logging.info('    Got contact info from database.')
                except (IndexError,KeyError):
                    contact['sde_instance'] = 'habsos'
                    contact['sde_table'] = 'HABSOS_CONTACTS'
                    contact_id = infoBroker.writeToSDE(contact)[0]
                    if 'Error' in contact_id:
                        raise InfobrokerError(contact_id['Error'])
                    else:
                        contact_id = contact_id['Success']
                    logging.info('    Wrote contact info to database.')
            else:
                #
                # This is a dry run, so simulate a response from the database
                #
                contact_id = contact_ctr
                contact_ctr += 1
            contact_ids.append(contact_id)

    return contact_id

#-------------------------------------------------------------------------------------

def getSampleSiteId(infoBroker, rowdata, options):
    """
    Description:    Return the primary key value of a sampling site record in the database.
    Input:	    infoBroker:	XMLRPC ServerProxy instance pointing to the Information Broker.
                    rowdata:  <dict>  A data record retrieved from the data source.
                    options:  <obj>   OptionParser command-line options.
    Return Value:   The primary key value of a contact record.
    Discussion:     The algorithm relies on the presence of certain key-value pairs in the 'rowdata'
                    parameter.  If none are present, no sampling site id will be returned.  If they are
                    present, the algorithm will first see if the record is stored in the local cache.
                    If not, the algorithm will query the database.  If not in the database, the algorithm
                    will insert the record.  The algorithm will store the sampling site record in the local
                    cache to avoid excessive queries on the database.
    """

    global sample_sites, samplesite_ids, samplesite_ctr

    samplesite_id = None
    sample_site = {'latitude':rowdata['LATITUDE'],
                   'longitude':rowdata['LONGITUDE']}
    try:
        #
        # Replace single and double quotes with two single quotes so the InfoBroker won't barf
        #
        sample_site['description'] = rowdata['DESCRIPTION'][:128].replace("'", "''").replace('"',"''")
    except KeyError:
        pass

    try:
        sample_site['ecoregion'] = rowdata['ECOREGION']
    except KeyError:
        pass

    try:
        sample_site['ecosystem'] = rowdata['ECOSYSTEM']
    except KeyError:
        pass

    try:
        sample_site['state_id'] = rowdata['STATE']
    except KeyError:
        pass

    logging.info('    Sample Site:')
    for k in sample_site:
        logging.info('      %s: %s'%(k, sample_site[k]))
    #
    # First see if the sampling site info is stored locally.
    #
    if sample_site in sample_sites:
        samplesite_id = samplesite_ids[sample_sites.index(sample_site)]
        logging.info('    Got sample site from list (id=%d).'%samplesite_id)
    else:
        #
        # Store the sampling site info in the local list, then query the database to see if it's there.
        # If it's not in the database, store it there.
        #
        sample_sites.append(sample_site)
        if not options.dry_run:
            try:
                samplesite_id = infoBroker.querySDE('habsos',
                                                    'SAMPLE_SITES',
                                                    'OBJECTID',
                                                    sample_site)
                if 'Error' in samplesite_id:
                    raise InfobrokerError(samplesite_id['Error'])
                else:
                    samplesite_id = samplesite_id[0]['OBJECTID']
                logging.info('  Got sample site from database.')
            except (IndexError,KeyError):
                sample_site.update({'sde_instance':'habsos',
                                    'sde_table':'SAMPLE_SITES',
                                    'shape':{'type':'point',
                                             'x':[rowdata['LONGITUDE']],
                                             'y':[rowdata['LATITUDE']]}})
                samplesite_id = infoBroker.writeToSDE(sample_site)[0]
                if 'Error' in samplesite_id:
                    raise InfobrokerError(samplesite_id['Error'])
                else:
                    samplesite_id = samplesite_id['Success']
                logging.info('  Wrote sample site to database.')
        else:
            #
            # This is a dry run, so simulate a response from the database
            #
            samplesite_id = samplesite_ctr
            samplesite_ctr += 1
        samplesite_ids.append(samplesite_id)

    return samplesite_id

#-------------------------------------------------------------------------------------

def processHABSData(infoBroker, processor, args, options):
    """
    Description:    Invoke a HABS data processor and insert any returned data into the SDE.
    Input:	    infoBroker:	<obj>   XMLRPC ServerProxy instance pointing to the Information Broker.
                    processor:  <obj>   A function object pointing to some processor method.
                    args:       <tuple> Function arguments for the processor function.
                    options:    <obj>   ConfigParser object containing command-line options.
    """

    logging.info('Using processor %s'%`processor.func_name`)
    packages = []
    for rowdata, qadata, qacomments in processor(*args):
        if rowdata == 'FLUSH':
            logging.info('Received FLUSH directive; %d packages in queue'%len(packages))
            flush(packages, infoBroker, options)
            packages = []
        else:
            #
            # Log what we got
            #
            logging.info('    Sample Data:')
            keys = rowdata.keys()
            keys.sort()
            for k in keys:
                if k == 'DATE':
                    logging.info('      %s: %s (%s)'%(k, rowdata[k], time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(rowdata[k]))))
                else:
                    logging.info('      %s: %s'%(k, rowdata[k]))
            logging.info('    QA Values:')
            keys = qadata.keys()
            keys.sort()
            for k in keys:
                logging.info('      %s: %s'%(k, qadata[k]))
            logging.info('    QA Comments:')
            keys = qacomments.keys()
            keys.sort()
            for k in keys:
                logging.info('      %s: %s'%(k, qacomments[k]))
            #
            # Accumulate any QA comments in a list, then join them into a semicolon-delimited string.
            #
            qaCommentVals = qacomments.values()
            try:
               qaCommentVals.append(rowdata['QA_COMMENT'])
            except KeyError:
               pass
            qacommentStr = ';'.join([x for x in qaCommentVals if (x and type(x) == types.StringType)])[:256]
            #
            # Format the sample date
            #
            datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(rowdata['DATE']))
            #
            # Collector/contact info
            #
            contact_id = getContactId(infoBroker, rowdata, options)
            #
            # Sampling site
            #
            samplesite_id = getSampleSiteId(infoBroker, rowdata, options)

            try:
               #
               # Replace single and double quotes with two single quotes so the InfoBroker won't barf
               #
               comment = rowdata['COMMENTS'].replace("'", "''").replace('"',"''")[:512]
            except (KeyError, AttributeError):
               comment = ''
            #
            # Build the package to be sent to the InfoBroker.
            #
            for organismName in rowdata['CELLCOUNTS'].keys():
                samples = {'sde_instance':'habsos',
                           'sde_table':'SAMPLES',
                           'sample_date':datetime,
                           'sample_site_id':samplesite_id,
                           'comments':comment,
                           'qa_comment':qacommentStr,
                           'organism_id':rowdata['ORGANISM_IDS'][organismName],
                           'cellcount_qa':qadata['CELLCOUNTS'][organismName],
                           'salinity_qa':qadata['SALINITY'],
                           'water_temp_qa':qadata['WTEMP'],
                           'wind_dir_qa':qadata['WDIR'],
                           'wind_speed_qa':qadata['WSPD'],
                          }
        
                if contact_id is not None:
                    samples['contact_id'] = contact_id

                try:
                    samples['category'] = rowdata['CATEGORIES'][organismName]
                except KeyError:
                    pass

                try:
                    samples['sample_depth'] = float(rowdata['DEPTH'])
                except (KeyError, ValueError):
                    pass

                try:
                    samples['cellcount'] = rowdata['CELLCOUNTS'][organismName]
                    samples['cellcount_unit'] = unitMap['CELLCOUNTS'][organismName]
                except KeyError:
                    pass

                try:
                    samples['salinity'] = rowdata['SALINITY']
                    samples['salinity_unit'] = unitMap['SALINITY']
                except KeyError:
                    pass

                try:
                    samples['water_temp'] = rowdata['WTEMP']
                    samples['water_temp_unit'] = unitMap['WTEMP']
                except KeyError:
                    pass

                try:
                    samples['wind_dir'] = rowdata['WDIR']
                    samples['wind_dir_unit'] = unitMap['WDIR']
                except KeyError:
                    pass

                try:
                    samples['wind_speed'] = rowdata['WSPD']
                    samples['wind_speed_unit'] = unitMap['WSPD']
                except KeyError:
                    pass

                try:
                    samples['habsos_id'] = rowdata['HABSOS_ID']
                except KeyError:
                    pass
                #
                # Log the InfoBroker package
                #
                logging.info('    Sample (to database):')
                keys = samples.keys()
                keys.sort()
                for k in keys:
                    logging.info('      %s: %s'%(k, samples[k])) 
                packages.append(samples)
                #
                # Send the data if this is a live run and log the return value
                #
                if len(packages) == 250:
                    flush(packages, infoBroker, options)
                    packages = []

            logging.info('----------------------------------------')

    if packages:
        flush(packages, infoBroker, options)
        packages = []

    logging.info('Done.')
