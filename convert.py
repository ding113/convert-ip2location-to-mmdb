import os
import sys
import ipaddress
import csv
import socket
import struct
import re
import binascii
import time
import json
import random
from tqdm import tqdm
import logging

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 定义常量
STRING_TYPE = 2 << 5
MAP_TYPE = 7 << 5
POINTER_TYPE = 1 << 5
UINT16_TYPE = 5 << 5
UINT32_TYPE = 6 << 5
UINT64_TYPE = 2
ARRAY_TYPE = 11 - 7
EXTENDED_TYPE = 0
DOUBLE_TYPE = 3 << 5

def no2ip(iplong):
    return socket.inet_ntoa(struct.pack('!I', int(iplong)))

def ip2no(ip):
    return struct.unpack("!I", socket.inet_aton(ip))[0]

def myprint(d):
    stack = list(d.items())
    visited = set()
    while stack:
        k, v = stack.pop()
        if isinstance(v, dict):
            if k not in visited:
                stack.extend(v.items())
            else:
                print("%s: %s" % (k, v))
            visited.add(k)

def travtree(hash_dict, level, trace):
    global data
    leftval = rightval = -1
    leftleaf = rightleaf = 0
    for k, v in sorted(hash_dict.items(), key=lambda x: random.random()):
        key2 = k
        trace2 = trace + key2
        if isinstance(v, str):
            if k == 'x0':
                leftval, leftleaf = v, 1
            elif k == 'x1':
                rightval, rightleaf = v, 1
        elif isinstance(v, dict):
            tmp = travtree(v, level + 1, trace2)
            if k == 'x0':
                leftval = tmp
            elif k == 'x1':
                rightval = tmp
    
    if level not in data:
        data[level] = {}

    ownoffset = len(data[level])
    data[level][ownoffset] = f"{leftval}#{rightval}"
    return ownoffset

def custom_sprintf(num1):
    return format(int(num1), '08b')

def print_double(num):
    return struct.pack(">d", float(num))

def print_byte(num):
    return struct.pack('B', int(num))

def print_uint(num):
    if num < 256:
        return struct.pack('>B', num)
    elif num < 65536:
        return struct.pack('>H', num)
    elif num < 4294967296:
        return struct.pack('>I', num)
    else:
        return struct.pack('>Q', num)

def print_pointer(num):
    if num < 2048:
        return struct.pack('>I', (1 << 30) | num)[1:]
    elif num < 526336:
        return struct.pack('>I', (2 << 30) | (num - 2048))[1:]
    elif num < 134217728:
        return struct.pack('>I', (3 << 30) | (num - 526336))
    else:
        raise ValueError("Pointer value too large")

def get_byte_array(num, bytes_count):
    return num.to_bytes(bytes_count, byteorder='big')

def print_node(leftdata, rightdata):
    global dbtype
    if dbtype == 'country':
        return struct.pack('>I', leftdata)[1:] + struct.pack('>I', rightdata)[1:]
    elif dbtype == 'city':
        return struct.pack('>II', leftdata, rightdata)

def keys_exists(element, *keys):
    if not isinstance(element, dict):
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except (KeyError, TypeError):
            return False
    return True

def write_metadata(f, metadata):
    f.write(struct.pack('B', MAP_TYPE | len(metadata)))
    for key, value in metadata.items():
        write_string(f, key)
        if isinstance(value, int):
            write_uint(f, value)
        elif isinstance(value, str):
            write_string(f, value)
        elif isinstance(value, dict):
            write_map(f, value)
        elif isinstance(value, list):
            write_array(f, value)

def write_string(f, s):
    b = s.encode('utf-8')
    f.write(struct.pack('B', STRING_TYPE | len(b)))
    f.write(b)

def write_uint(f, num):
    if num < 256:
        f.write(struct.pack('BB', UINT16_TYPE | 1, num))
    elif num < 65536:
        f.write(struct.pack('>BH', UINT16_TYPE | 2, num))
    elif num < 4294967296:
        f.write(struct.pack('>BI', UINT32_TYPE | 4, num))
    else:
        f.write(struct.pack('>BQ', UINT64_TYPE | 8, num))

def write_map(f, m):
    f.write(struct.pack('B', MAP_TYPE | len(m)))
    for k, v in m.items():
        write_string(f, k)
        if isinstance(v, str):
            write_string(f, v)
        elif isinstance(v, int):
            write_uint(f, v)

def write_array(f, arr):
    f.write(struct.pack('BB', EXTENDED_TYPE | 0, ARRAY_TYPE))
    f.write(struct.pack('B', len(arr)))
    for item in arr:
        write_string(f, item)

def main():
    global data, dbtype
    tokens = {"country": 0, "iso_code": 0, "names": 0, "en": 0, "-": 0}
    tokens2 = {"city": 0, "location": 0, "postal": 0, "latitude": 0, "longitude": 0, "code": 0, "subdivisions": 0}
    latlongs = {}
    cities = {}
    countries = {}
    cidrdata = []
    sortbylength = {}
    countryoffset = {}
    cityoffset = {}
    btree = {}
    data = {}
    datastartmarker = b'\x00' * 16
    datastartmarkerlength = len(datastartmarker)

    if len(sys.argv) > 1:
        filename = sys.argv[1]
        if filename.lower().endswith('.csv'):
            with open(filename, 'r', encoding='utf-8') as f:
                mycsv = csv.reader(f)
                for row in tqdm(mycsv, desc="Processing CSV"):
                    therest = ''
                    if len(row) == 10:
                        dbtype = 'city'
                        for i in range(2, 6):
                            tokens[row[i]] = 0
                        latlongs[row[6]] = 0
                        latlongs[row[7]] = 0
                        tokens[row[8]] = 0
                        cities["|".join(row[2:9])] = 0
                        therest = "|".join([row[2], row[4], row[5], row[6], row[7], row[8]])
                    else:
                        dbtype = 'country'
                        countries[row[2]] = row[3]
                        therest = row[2]
                    fromip = ip2no(row[0])
                    toip = ip2no(row[1])
                    startip = ipaddress.IPv4Address(fromip)
                    endip = ipaddress.IPv4Address(toip)
                    ar = [ipaddr for ipaddr in ipaddress.summarize_address_range(startip, endip)]
                    ar1 = sorted(str(cidr) for cidr in ar)
                    for cidr in ar1:
                        cidrdata.append(f'"{cidr}",{therest}')

            logging.info("Processing CIDR data")
            for entry in tqdm(cidrdata, desc="Processing CIDR entries"):
                regex_here1 = r"^\"([\d\.]+)\/(\d+)\",(.*)"
                match = re.search(regex_here1, entry)
                if match:
                    ip, cidr, line_copy1 = match.groups()
                    iparr = ip.split('.')
                    binary = list(map(custom_sprintf, iparr))
                    binarystr = "".join(binary)
                    binarystrcidr = binarystr[0:int(cidr)]
                    sortbylength["GG" + binarystrcidr] = line_copy1

            logging.info("Constructing data section")
            datasection = b""
            if dbtype == 'city':
                tokens.update(tokens2)

            for key in tqdm(sorted(tokens), desc="Processing tokens"):
                tokens[key] = len(datasection)
                tokenlength = len(key)
                controlbyte = STRING_TYPE | tokenlength
                datasection += print_byte(controlbyte) + key.encode()

            for key1 in tqdm(sorted(latlongs), desc="Processing lat/long"):
                latlongs[key1] = len(datasection)
                controlbyte1 = DOUBLE_TYPE | 8
                datasection += print_byte(controlbyte1) + print_double(float(key1))

            if dbtype == 'country':
                for key2 in tqdm(sorted(countries), desc="Processing countries"):
                    countryoffset[key2] = len(datasection)
                    
                    controlbyte = MAP_TYPE | 1
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["country"])
                    
                    controlbyte = MAP_TYPE | 2
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["iso_code"])
                    
                    tokenlength = len(key2)
                    controlbyte = STRING_TYPE | tokenlength
                    datasection += print_byte(controlbyte) + key2.encode()
                    datasection += print_pointer(tokens["names"])
                    
                    controlbyte = MAP_TYPE | 1
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["en"])
                    
                    countryname = countries[key2]
                    tokenlength = len(countryname)
                    controlbyte = STRING_TYPE | tokenlength
                    datasection += print_byte(controlbyte) + countryname.encode()
            elif dbtype == 'city':
                for key2 in tqdm(sorted(cities), desc="Processing cities"):
                    array = key2.split('|')
                    countrycode, countryname, statename, cityname, latitude, longitude, postcode = array
                    cityoffset[f"{countrycode}|{statename}|{cityname}|{latitude}|{longitude}|{postcode}"] = len(datasection)
                    controlbyte = MAP_TYPE | 5
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["city"])
                    controlbyte = MAP_TYPE | 1
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["names"])
                    controlbyte = MAP_TYPE | 1
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["en"])
                    datasection += print_pointer(tokens[cityname])
                    
                    datasection += print_pointer(tokens["country"])
                    controlbyte = MAP_TYPE | 2
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["iso_code"])
                    datasection += print_pointer(tokens[countrycode])
                    datasection += print_pointer(tokens["names"])
                    controlbyte = MAP_TYPE | 1
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["en"])
                    datasection += print_pointer(tokens[countryname])
                    
                    datasection += print_pointer(tokens["location"])
                    controlbyte = MAP_TYPE | 2
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["latitude"])
                    datasection += print_pointer(latlongs[latitude])
                    datasection += print_pointer(tokens["longitude"])
                    datasection += print_pointer(latlongs[longitude])
                    
                    datasection += print_pointer(tokens["postal"])
                    controlbyte = MAP_TYPE | 1
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["code"])
                    datasection += print_pointer(tokens[postcode])
                    
                    datasection += print_pointer(tokens["subdivisions"])
                    myint = 1
                    controlbyte = EXTENDED_TYPE | myint
                    typebyte = ARRAY_TYPE
                    datasection += print_byte(controlbyte) + print_byte(typebyte)
                    controlbyte = MAP_TYPE | 1
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["names"])
                    controlbyte = MAP_TYPE | 1
                    datasection += print_byte(controlbyte)
                    datasection += print_pointer(tokens["en"])
                    datasection += print_pointer(tokens[statename])

            logging.info("Updating B-tree")
            for binarystrcidr in tqdm(sorted(sortbylength), desc="Updating B-tree"):
                tmp_modify = binarystrcidr[2:]
                current = btree
                for bit in tmp_modify:
                    key = 'x' + bit
                    if key not in current:
                        current[key] = {}
                    current = current[key]
                current[key] = sortbylength[binarystrcidr]

            logging.info("Traversing B-tree")
            travtree(btree, 0, '')

            totalnodes = sum(len(level) for level in data.values())
            offsetnodes = {i: sum(len(data[j]) for j in range(i+1)) for i in range(len(data))}

            logging.info("Writing to file")
            filename2 = filename + '.MMDB'
            with open(filename2, 'wb') as f:
                # Write MMDB header
                f.write(b'\x00' * 16)
                f.write(b'MaxMind.com')
                f.write(bytes([0xab, 0xcd, 0xef]))

                for i in tqdm(range(len(data)), desc="Writing nodes"):
                    for nodedata in data[i].values():
                        left, right = map(str, nodedata.split('#'))
                        leftdata = rightdata = 0
                        if left.isdigit():
                            leftdata = int(left) + offsetnodes[i]
                        else:
                            if dbtype == 'country':
                                leftdata = countryoffset.get(left, 0) + datastartmarkerlength + totalnodes if not (left.replace('-','',1).isdigit() and int(left) < 0) else datastartmarkerlength + totalnodes
                            elif dbtype == 'city':
                                leftdata = cityoffset.get(left, 0) + datastartmarkerlength + totalnodes if not (left.replace('-','',1).isdigit() and int(left) < 0) else datastartmarkerlength + totalnodes
                        
                        if right.isdigit():
                            rightdata = int(right) + offsetnodes[i]
                        else:
                            if dbtype == 'country':
                                rightdata = countryoffset.get(right, 0) + datastartmarkerlength + totalnodes if not (right.replace('-','',1).isdigit() and int(right) < 0) else datastartmarkerlength + totalnodes
                            elif dbtype == 'city':
                                rightdata = cityoffset.get(right, 0) + datastartmarkerlength + totalnodes if not (right.replace('-','',1).isdigit() and int(right) < 0) else datastartmarkerlength + totalnodes
                        
                        f.write(print_node(leftdata, rightdata))

                f.write(datastartmarker)
                f.write(datasection)
                f.write(binascii.unhexlify(b'ABCDEF4D61784D696E642E636F6D'))

                # Write metadata
                metadata = {
                    "binary_format_major_version": 2,
                    "binary_format_minor_version": 0,
                    "build_epoch": int(time.time()),
                    "database_type": f"IP2LITE-{dbtype.capitalize()}",
                    "description": {"en": f"IP2LITE-{dbtype.capitalize()} database"},
                    "ip_version": 4,
                    "languages": ["en"],
                    "node_count": totalnodes,
                    "record_size": 24 if dbtype == 'country' else 28
                }

                write_metadata(f, metadata)

            logging.info(f"You have successfully converted {filename} to {filename2}.")
            print(f"You can now use {filename2} with any MaxMind API which supports the GeoLite2 format.\n")

        else:
            raise Exception('Only .csv files are accepted.')
    else:
        print("Usage: python3 convert.py <IP2Location LITE DB1 or DB11 CSV file>\n")
        raise Exception('Please enter a filename.')

if __name__ == '__main__':
    main()
