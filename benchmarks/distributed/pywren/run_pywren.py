import time
import logging
import traceback
import os
os.environ['PYWREN_LOGLEVEL']='INFO'
import io
import boto3
import csv

start_time = time.time()
import pywren

logging.info('PyWren init took {}s'.format(time.time() - start_time))

# Functions to extract relevant fields from input data
# taken from zillow/Z1...
def extractZipcode(x):
    try:
        return x + ('{:05}'.format(int(float(x[4]))),)
    except:
        return x + (None,)

def cleanCity(x):
    try:
        return x[:2] + (x[2][0].upper() + x[2][1:].lower(),) + x[3:]
    except:
        return x[:2] + (None,) + x[3:]

def extractBd(x):
    try:
        val = x[6]
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
        return x + (int(r),)
    except:
        return x + (None,)


def extractBa(x):
    try:
        val = x[6]
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
        return x + (int(r),)
    except:
        return x + (None,)


def extractSqft(x):
    try:
        val = x[6]
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
        return x + (int(r),)
    except:
        return x + (None,)


def extractOffer(x):
    try:
        offer = x[0].lower()
        if 'sale' in offer:
            return x + ('sale',)
        if 'rent' in offer:
            return x + ('rent',)
        if 'sold' in offer:
            return x + ('sold',)
        if 'foreclose' in offer.lower():
            return x + ('foreclosed',)
        return x + (offer,)
    except:
        return x + (None,)


def extractType(x):
    try:
        t = x[0].lower()
        type = 'unknown'
        if 'condo' in t or 'appartment' in t:
            type = 'condo'
        if 'house' in t:
            type = 'house'
        return x + (type,)
    except:
        return x + (None,)


def extractPrice(x):
    try:
        price = x[5]
        if x[15] == 'sold':
            # price is to be calculated using price/sqft * sqft
            val = x[6]
            s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]
            r = s[s.find('$') + 1:s.find(', ') - 1]
            price_per_sqft = int(r)
            price = price_per_sqft * x['sqft']
        elif x[15] == 'rent':
            max_idx = price.rfind('/')
            price = int(price[1:max_idx].replace(',', ''))
        else:
            # take price from price column
            price = int(price[1:].replace(',', ''))

        return x[:5] + (price,) + x[6:]
    except:
        return x[:5] + (None,) + x[6:]


def selectCols(x):
    return (x[8], x[12], x[1], x[2], x[3], x[10], x[13], x[14], x[15], x[11], x[5])


def filterPrice(x):
    try:
        return 100000 < x[5] <= 2e7
    except:
        return False


def filterType(x):
    try:
        return x[-1] == 'house'
    except:
        return False


def filterBd(x):
    try:
        return x[-1] < 10
    except:
        return False

def csv2tuples(file_obj, header=True):
    reader = csv.reader(file_obj, delimiter=',', quotechar='"')
    rows = [tuple(row) for row in reader]
    if header:
        return rows[1:]
    else:
        return rows

def tocsvstr(a):
    a = [str(x) for x in a]
    return ','.join(a) + '\n'

def s3_split_uri(uri):
    assert '/' in uri, 'at least one / is required!'
    uri = uri.replace('s3://', '')

    bucket = uri[:uri.find('/')]
    key = uri[uri.find('/') + 1:]
    return bucket, key

def pipeline(args):
    input_path, output_path = args
    # this here is the original pipeline written using Tuplex's high-level API
    # ctx.csv(args.data_path) \
    #    .withColumn("bedrooms", extractBd) \
    #    .filter(lambda x: x["bedrooms"] < 10) \
    #    .withColumn("type", extractType) \
    #    .filter(lambda x: x["type"] == "house") \
    #    .withColumn("zipcode", lambda x: "%05d" % int(x["postal_code"])) \
    #    .mapColumn("city", lambda x: x[0].upper() + x[1:].lower()) \
    #    .withColumn("bathrooms", extractBa) \
    #    .withColumn("sqft", extractSqft) \
    #    .withColumn("offer", extractOffer) \
    #    .withColumn("price", extractPrice) \
    #    .filter(lambda x: 100000 < x["price"] < 2e7) \
    #    .selectColumns(["url","zipcode","address","city","state","bedrooms","bathrooms","sqft","offer","type","price"]) \
    #    .tocsv(args.output_path)

    # processing using python's tuple based functions...
    # and AWS boto3 to download/upload files...
    try:
        start_time = time.time()

        s3_client = boto3.client('s3', region_name='us-east-1')
        local_path = '/tmp/tmp.csv' # MAXIMUM is 512MB!
        bucket, key = s3_split_uri(input_path)

        # s3_client.download_file(bucket, key, local_path)
        buf = io.BytesIO()
        s3_client.download_fileobj(bucket, key, buf)
        utf_wrapper = io.TextIOWrapper(buf, encoding='utf-8')
        records = csv2tuples(utf_wrapper)
        load_time = time.time() - start_time

        start_time = time.time()
        # trafo steps
        step_1 = map(extractBd, records)
        step_2 = filter(filterBd, step_1)
        step_3 = map(extractType, step_2)
        step_4 = filter(filterType, step_3)
        step_5 = map(extractZipcode, step_4)
        step_6 = map(cleanCity, step_5)
        step_7 = map(extractBa, step_6)
        step_8 = map(extractSqft, step_7)
        step_9 = map(extractOffer, step_8)
        step_10 = map(extractPrice, step_9)
        step_11 = filter(filterPrice, step_10)
        step_12 = map(selectCols, step_11)

        res = list(step_12)
        run_time = time.time() - start_time
        start_time = time.time()

        columns = ['url', 'zipcode', 'address', 'city', 'state', 'bedrooms', 'bathrooms',
                   'sqft', 'offer', 'type', 'price']

        # write to file & then upload to S3
        with open(local_path, 'w') as fp:
            fp.write(','.join(columns) + '\n')
            for row in map(tocsvstr, res):
                fp.write(row)

        # upload to S3
        bucket, key = s3_split_uri(output_path)
        s3_client.upload_file(local_path, bucket, key)

        write_time = time.time() - start_time

        job_time = load_time + run_time + write_time

        stats = {'type': 'tuple',
                 'framework': 'pywren',
                 'input_files': [input_path],
                 'load_time': load_time,
                 'run_time': run_time,
                 'write_time': write_time,
                 'job_time' : job_time,
                 'num_input_rows' : len(records),
                 'num_output_rows':len(res)}

        return stats
    except Exception as e:
        return {'exception' : str(type(e)),
                'details' : str(e),
                'traceback': traceback.format_exc(),
                'input_path' : input_path,
                'output_path' : output_path}

# from https://alexwlchan.net/2017/07/listing-s3-keys/
def get_all_s3_keys(s3_client, uri):
    """Get a list of all keys in an S3 bucket."""
    keys = []

    bucket, key = s3_split_uri(uri)
    kwargs = {'Bucket': bucket, 'Prefix': key}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys


wrenexec = pywren.default_executor()

# this is an invocation example for a SINGLE file.
# args = ('s3://tuplex-public/data/100GB/zillow_00001.csv', 's3://pywren-leonhard/zillow_test_output.csv')
# future = wrenexec.call_async(pipeline, args)
# res = future.result()


# yet, we want to have the result for ALL files...
# first need to list root path
root_uri = 's3://tuplex-public/data/100GB/'
s3_client = boto3.client('s3')
keys = get_all_s3_keys(s3_client, root_uri)
logging.info('Found {} keys'.format(len(keys)))
tuples = list(map(lambda key: ('s3://tuplex-public/' + key, 's3://pywren-leonhard/wren-job/output.part.{}'.format(key[key.rfind('_')+1:])), keys))
print(tuples[:3])
logging.info('Starting PyWren map...')
futures = wrenexec.map(pipeline, tuples)
results = pywren.get_all_results(futures)
logging.info('PyWren completed, stats:')
print(results[:3])
logging.info('PyWren Query took {}s'.format(time.time() - start_time))

# write results?