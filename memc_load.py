#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
import concurrent.futures
import time

NORMAL_ERR_RATE = 0.01
MAX_WORKERS = 10
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])
_memcached_clients = {}
SOCKET_TIMEOUT = 3


def get_memc_client(memc_addr):
    if memc_addr not in _memcached_clients:
        _memcached_clients[memc_addr] = memcache.Client([memc_addr], socket_timeout=SOCKET_TIMEOUT)
    return _memcached_clients[memc_addr]


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False, max_retries=3, retry_timeout=1):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()

    for attempt in range(1, max_retries + 1):
        try:
            if dry_run:
                str_ua = str(ua).replace('\n', ' ')
                logging.debug(f"{memc_addr} - {key} -> {str_ua}")
                return True
            memc = get_memc_client(memc_addr)
            result = memc.set(key, packed)
            if result:
                return True
            else:
                logging.warning(f"Memcache set returned False, attempt {attempt}, key={key}, memc_addr={memc_addr}")
        except Exception as e:
            logging.exception(f"Memcache write error, attempt {attempt}, key={key}, memc_addr={memc_addr}: {e}")
        if attempt < max_retries:
            time.sleep(retry_timeout)
    return False


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    files = sorted(glob.glob(options.pattern))
    for fn in files:
        processed = 0
        errors = 0
        logging.info(f'Processing {fn}')
        with gzip.open(fn, 'rt') as fd:

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for line in fd:
                    line = line.strip()
                    if not line:
                        continue
                    appsinstalled = parse_appsinstalled(line)
                    if not appsinstalled:
                        errors += 1
                        continue
                    memc_addr = device_memc.get(appsinstalled.dev_type)
                    if not memc_addr:
                        errors += 1
                        logging.error("Unknown device type: %s" % appsinstalled.dev_type)
                        continue
                    futures.append(
                        executor.submit(insert_appsinstalled, memc_addr, appsinstalled, options.dry)
                    )

            for future in concurrent.futures.as_completed(futures):
                try:
                    ok = future.result()
                    if ok:
                        processed += 1
                    else:
                        errors += 1
                except Exception as e:
                    logging.exception(f"Exception in thread: {e.__class__.__name__}, {e}")
                    errors += 1

        if not processed:
            dot_rename(fn)
            continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info(f"Acceptable error rate {err_rate}. Successful load")
        else:
            logging.error(f"High error rate ({err_rate} > {NORMAL_ERR_RATE}). Failed load")
        dot_rename(fn)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
    finally:
        _memcached_clients.clear()
