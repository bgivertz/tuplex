#!/usr/bin/env python3
# (c) L.Spiegelberg 2021
# conduct dirty Zillow data experiment as described in README.md

import tuplex
import time
import sys
import json
import os
import glob
import argparse
import math
import re

# UDFs for pipeline
def extractBd(x):
    val = x['facts and features']
    max_idx = val.find(' bd')
    if max_idx < 0:
        max_idx = len(val)
    s = val[:max_idx]

    # find comma before
    split_idx = s.rfind(',')
    if split_idx < 0:
        split_idx = 0
    else:
        split_idx += 2
    r = s[split_idx:]
    return int(r)

def extractBa(x):
    val = x['facts and features']
    max_idx = val.find(' ba')
    if max_idx < 0:
        max_idx = len(val)
    s = val[:max_idx]

    # find comma before
    split_idx = s.rfind(',')
    if split_idx < 0:
        split_idx = 0
    else:
        split_idx += 2
    r = s[split_idx:]

    ba = math.ceil(2.0 * float(r)) / 2.0
    return ba

def extractSqft(x):
    val = x['facts and features']
    max_idx = val.find(' sqft')
    if max_idx < 0:
        max_idx = len(val)
    s = val[:max_idx]

    split_idx = s.rfind('ba ,')
    if split_idx < 0:
        split_idx = 0
    else:
        split_idx += 5
    r = s[split_idx:]
    r = r.replace(',', '')
    return int(r)

def extractOffer(x):
    offer = x['title'].lower()
    if 'sale' in offer:
        return 'sale'
    if 'rent' in offer:
        return 'rent'
    if 'sold' in offer:
        return 'sold'
    if 'foreclose' in offer.lower():
        return 'foreclosed'
    return offer

def extractType(x):
    t = x['title'].lower()
    type = 'unknown'
    if 'condo' in t or 'apartment' in t:
        type = 'condo'
    if 'house' in t:
        type = 'house'
    return type

def extractPrice(x):
    price = x['price']
    p = 0
    if x['offer'] == 'sold':
        # price is to be calculated using price/sqft * sqft
        val = x['facts and features']
        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]
        r = s[s.find('$')+1:s.find(', ') - 1]
        price_per_sqft = int(r)
        p = price_per_sqft * x['sqft']
    elif x['offer'] == 'rent':
        max_idx = price.rfind('/')
        p = int(price[1:max_idx].replace(',', ''))
    else:
        # take price from price column
        p = int(price[1:].replace(',', ''))

    return p

def resolveBd(x):
    if 'Studio' in x['facts and features']:
        return 1
    raise ValueError

def resolveBa(x):
    val = x['facts and features']
    max_idx = val.find(' ba')
    if max_idx < 0:
        max_idx = len(val)
    s = val[:max_idx]

    # find comma before
    split_idx = s.rfind(',')
    if split_idx < 0:
        split_idx = 0
    else:
        split_idx += 2
    r = s[split_idx:]
    return math.ceil(float(r))

def dirty_zillow_pipeline(paths, output_path, step):
    ds = ctx.csv(','.join(paths))
    ds = ds.withColumn("bedrooms", extractBd)
    if step > 0:
        ds = ds.resolve(ValueError, resolveBd)
        ds = ds.ignore(ValueError)
    ds = ds.filter(lambda x: x['bedrooms'] < 10)
    ds = ds.withColumn("type", extractType)
    ds = ds.filter(lambda x: x['type'] == 'condo')
    ds = ds.withColumn("zipcode", lambda x: '%05d' % int(x['postal_code']))
    if step > 1:
        ds = ds.ignore(TypeError)
    ds = ds.mapColumn("city", lambda x: x[0].upper() + x[1:].lower())
    ds = ds.withColumn("bathrooms", extractBa)
    if step > 2:
        ds = ds.ignore(ValueError)
    ds = ds.withColumn("sqft", extractSqft)
    if step > 3:
        ds = ds.ignore(ValueError)
    ds = ds.withColumn("offer", extractOffer)
    ds = ds.withColumn("price", extractPrice)
    if step > 4:
        ds = ds.resolve(ValueError, lambda x: int(re.sub('[^0-9.]*', '', x['price'])))
    ds = ds.filter(lambda x: 100000 < x['price'] < 2e7 and x['offer'] == 'sale')
    ds = ds.selectColumns(["url", "zipcode", "address", "city", "state",
                            "bedrooms", "bathrooms", "sqft", "offer", "type", "price"])
    ds.tocsv(output_path)
    return ctx.metrics

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Zillow cleaning')
    parser.add_argument('--path', type=str, dest='data_path', default='data/zillow_dirty.csv',
                        help='path or pattern to zillow data')
    parser.add_argument('--output-path', type=str, dest='output_path', default='tuplex_output/',
                        help='specify path where to save output data files')
    parser.add_argument('--resolve-in-order', dest='resolve_in_order', action="store_true", help="whether to resolve exceptions in order")
    parser.add_argument('--incremental-resolution', dest='incremental_resolution', action="store_true", help="whether to use incremental resolution")
    parser.add_argument('--single-threaded', dest='single_threaded', action="store_true", help="whether to use a single thread for execution")
    args = parser.parse_args()

    assert args.data_path, 'need to set data path!'

    # config vars
    paths = [args.data_path]
    output_path = args.output_path

    # explicit globbing because dask can't handle patterns well...
    if not os.path.isfile(args.data_path):
        paths = sorted(glob.glob(os.path.join(args.data_path, '*.csv')))
    else:
        paths = [args.data_path]

    if not paths:
        print('found no zillow data to process, abort.')
        sys.exit(1)

    print('>>> running {} on {}'.format('tuplex', paths))

    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    conf = {"webui.enable" : False,
            "executorCount" : 16,
            "executorMemory" : "6G",
            "driverMemory" : "6G",
            "partitionSize" : "32MB",
            "runTimeMemory" : "128MB",
            "useLLVMOptimizer" : True,
            "optimizer.nullValueOptimization" : False,
            "csv.selectionPushdown" : True,
            "optimizer.generateParser" : False} # bug when using generated parser. Need to fix that.

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    if args.incremental_resolution:
        conf["optimizer.incrementalResolution"] = True
    else:
        conf["optimizer.incrementalResolution"] = False

    if args.resolve_in_order:
        conf['optimizer.mergeExceptionsInOrder'] = True
    else:
        conf['optimizer.mergeExceptionsInOrder'] = False

    if args.single_threaded:
        conf['executorCount'] = 0
        conf['driverMemory'] = '32G'

    # Note: there's a bug in the merge in order mode here -.-
    # force to false version
    print('>>> bug in generated parser, force to false')
    conf["optimizer.generateParser"] = False

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()

    # decide which pipeline to run based on argparse arg!
    num_steps = 6
    metrics = []
    for step in range(num_steps):
        print(f'>>> running pipeline with {step} resolver(s) enabled...')
        m = dirty_zillow_pipeline(paths, output_path, step)
        print(m.as_json())
        metrics.append(m)

    run_time = time.time() - tstart
    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))

    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time,
                      'mode' : args.mode, 'mergeExceptionsInOrder': conf['optimizer.mergeExceptionsInOrder']}))
