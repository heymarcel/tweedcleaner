#!/usr/bin/python
"""
tweedcleaner.py: Grabs a list of feeds and cleans them up for tweetfeed
"""
__author__    = "Marcel Levy (heymarcel@gmail.com)"
__version__   = "0.1"
__date__      = "2008-12-27"
__copyright__ = "(c) Copyright 2008 Marcel Levy. All Rights Reserved. "


from optparse import OptionParser
import datetime, feedparser, logging, logging.handlers, os, PyRSS2Gen, \
    sys, shelve, time, urllib, yaml


options = False

logger = logging.getLogger('tweedlog')
logger.setLevel(logging.DEBUG)

def main():
    global logger
    global options
    
    usage = "usage: %prog [options] inputfile [ inputfile ... ]"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                      default=False, help="Make this program more chatty")
    parser.add_option("-V", "--version", action="store_true", dest="version",
                      default=False, help="Print the version number and exit")
    parser.add_option("-t", "--test", action="store_true", dest="test",
                      default=False, help="Download original feed for verification")
    parser.add_option("-c", "--config", dest="config",
                      help="configuration file (default \"./config.yaml\")",
                      metavar="CONFIG", default="./config.yaml")
    parser.add_option("-a", "--cache", dest="cache", type="string",
              help="cache file (default \"./cache.tweed\")",
              metavar="CACHE", default="./cache.tweed")
    
    (options, args) = parser.parse_args()
    
    if options.version:
        print "tweedcleaner.py %s" % __version__
        sys.exit(0)
    
    # Initial script does the following:
    
    #  Reads config.yaml file to get list of feeds
    try:
        configdoc = file(options.config, "r")
        config    = yaml.load(configdoc)
    except:
        print "Could not open configuration file '%s'" % options.config
        sys.exit(1)

    logfile = config["logdir"] + "tweed.log"
    loghandler = logging.handlers.RotatingFileHandler(logfile, maxBytes=2000000, backupCount=5)

    # create formatter
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    # add formatter to ch
    loghandler.setFormatter(formatter)

    logger.addHandler(loghandler)
    
    #  unpickle the cache (key = url)
    cache = shelve.open(options.cache)
    
    #  create cache-delete array from cache keys
    keys_to_kill = {}
    
    for k, v in cache.iteritems():
        keys_to_kill[k] = True
    
    for feed in config["feeds"]:
        if options.verbose:
            print "Grabbing %s" % feed["url"]
        logger.info("Grabbing %s" % feed["url"])

        #  Grab each feed, put into a string object
        try:
            rawfeed_file = urllib.urlopen(feed["url"])
            rawfeed = rawfeed_file.read()
            f = feedparser.parse(rawfeed)
        except:
            logger.error("Could not grab and parse feed: %s" % feed["url"])
            continue

        items = []
        for item in f["entries"]:
            # If item doesn't have a guid field, add its url
            if options.verbose:
                print item

            if not item.link.startswith("http://"):
                if 'link' in f.feed:
                    item.link = "%s/%s" % (f.feed.link, item.link)
                else:
                    # We have no time for malformed crap
                    logger.warning("malformed <link>: %s" % item.link)
                    continue

            if 'id' not in item:
                item.id = item.link
            
            # If item doesn't have a pubDate, check cache for
            # last-modified datefetch
            if options.verbose:
                print item.link
            itemkey = item.link.encode("utf-16le")

            if 'date' not in item:
                if itemkey in cache:
                    item.date = cache[itemkey]
                else:
                    # If cache miss, check header for url and use
                    # last-modified field from that
                    item.date = header_date(item.link)
                    # cache the last-modified field for the url
                    cache[itemkey] = item.date
            # Remove item url from keys-to-kill list
            if itemkey in keys_to_kill:
                del(keys_to_kill[itemkey])
            
            # Construct item
            items.append(   PyRSS2Gen.RSSItem(
                                title       = item.title,
                                link        = item.link,
                                description = item.description,
                                guid        = PyRSS2Gen.Guid(item.id, False),
                                pubDate     = item.date
                            )
                        )
        # TODO: Handle completely malformed feeds (as in ones without a title)
        # Construct output feed here
        rss = PyRSS2Gen.RSS2(
                title            = f.feed.title,
                link             = f.feed.link,
                description      = f.feed.description,
                lastBuildDate    = datetime.datetime.now(),
                items            = items
                )

        # Make sure output directory exists
        try:
            os.makedirs(config["outputdir"])
        except:
            if options.verbose:
                print "Output directory cannot be created or already exists."        
        # Write cleaned feed to file specified in config.yaml
        outfile = os.path.join(config["outputdir"], "%s.xml" % feed["name"])
        rss.write_xml(open(outfile, "w"))
        logger.info("Wrote %s" % outfile)
        if options.test:
            testname = os.path.join(config["outputdir"], "%s-test.xml" % feed["name"])
            testout = open(testname, "w")
            testout.write(rawfeed)
            logger.info("Original feed in %s" % testname)
    
    #  For each url in cache-delete array, delete key from cache
    for key in keys_to_kill:
        logger.debug("Deleting %s from cache" % key)
        del(cache[key])
    cache.close()


def header_date(url):
    """Grabs Last-Modified header from feed item, if available"""
    date = datetime.datetime.now()
    item = urllib.urlopen(url)
    rawdate = item.info().getheader('Last-Modified')
    if rawdate:
        date = datetime.datetime(*time.strptime(rawdate)[0:5])
    return date


if __name__ == '__main__':
    main()
