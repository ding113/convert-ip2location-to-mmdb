"""
IP2Location to MaxMind MMDB Converter

This script converts IP2Location LITE CSV database to Maxmind MMDB format.
It supports both country-level and city-level databases.

Usage:
    python3 convert.py <IP2Location LITE DB1 or DB11 CSV file>

Requirements:
    - Python 3.5+
    - tqdm library for progress bars

This script is based on the original work by antonvlad999 (https://github.com/antonvlad999/convert-ip2location-geolite2)
and has been modified and improved for better performance and usability.
"""

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

# 定义常量 - 这些常量用于MMDB文件格式中的不同数据类型
STRING_TYPE = 2 << 5
MAP_TYPE = 7 << 5
POINTER_TYPE = 1 << 5
UINT16_TYPE = 5 << 5
UINT32_TYPE = 6 << 5
UINT64_TYPE = 9 - 7
ARRAY_TYPE = 11 - 7
EXTENDED_TYPE = 0
DOUBLE_TYPE = 3 << 5

def no2ip(iplong):
    """Convert a long integer to an IP address string."""
    return socket.inet_ntoa(struct.pack('!I', int(iplong)))

def ip2no(ip):
    """Convert an IP address string to a long integer."""
    return struct.unpack("!I", socket.inet_aton(ip))[0]

def myprint(d):
    """Print a nested dictionary."""
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
    """
    Traverse the binary tree and build the data structure.
    This function is crucial for creating the MMDB search tree.
    """
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
    """Convert an integer to its binary string representation."""
    return format(int(num1), '08b')

def print_double(num):
    """Convert a float to its binary representation."""
    return struct.pack(">d", float(num))

def print_byte(num):
    """Convert an integer to a single byte, removing null bytes."""
    return struct.pack('I', int(num)).rstrip(b'\x00')

def print_byte1(num):
    """Convert an integer to a single byte."""
    return struct.pack('B', int(num))

def print_uint(num):
    """Convert an unsigned integer to its binary representation."""
    s = b""
    while num > 0:
        num2 = int(num) & 0xFF
        s = print_byte(num2) + s
        num = int(num) >> 8
    return s

def print_pointer(num):
    """
    Create a pointer for the MMDB format.
    This is used to reference data within the database.
    """
    global POINTER_TYPE
    pointersize = -1
    threebits = 0
    balance = []

    if num <= 2047:
        pointersize = 0
        threebits = num >> 8
        balance = get_byte_array(num, 1)
    elif num <= 526335:
        pointersize = 1
        num = num - 2048
        threebits = num >> 16
        balance = get_byte_array(num, 2)
    elif num <= 134744063:
        pointersize = 2
        num = num - 526336
        threebits = num >> 24
        balance = get_byte_array(num, 3)
    elif num <= 4294967295:
        pointersize = 3
        threebits = 0
        balance = get_byte_array(num, 4)
    else:
        raise Exception("Pointer value too large.\n")

    pointersize = pointersize << 3
    controlbyte = POINTER_TYPE | pointersize | threebits
    s = print_byte(controlbyte)
    for i in range(len(balance)):
        s += print_byte(balance[i])
    return s

def get_byte_array(num, bytes_count):
    """Convert a number to an array of bytes."""
    bytesarr = []
    for i in range(bytes_count):
        tmp = int(num) & 0xFF
        num = int(num) >> 8
        bytesarr = ([tmp] + bytesarr)
    return bytesarr

def print_node(leftdata, rightdata):
    """
    Create a node for the MMDB search tree.
    The format differs slightly between country and city databases.
    """
    global dbtype
    mybytes = []
    leftbytes = []
    rightbytes = []
    
    if dbtype == 'country':
        leftbytes = get_byte_array(leftdata, 3)
        rightbytes = get_byte_array(rightdata, 3)
        mybytes = leftbytes + rightbytes
    elif dbtype == 'city':
        leftbytes = get_byte_array(leftdata, 4)
        rightbytes = get_byte_array(rightdata, 4)
        midbyte = (leftbytes[0] << 4) ^ rightbytes[0]
        leftbytes = leftbytes[1:]
        rightbytes = rightbytes[1:]
        leftbytes.append(midbyte)
        mybytes = leftbytes + rightbytes
    
    s = b""
    for i in range(len(mybytes)):
        s += print_byte1(mybytes[i])

    return s

def keys_exists(element, *keys):
    """Check if a series of keys exists in a nested dictionary."""
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

def main():
    global data, dbtype, POINTER_TYPE
    
    # 初始化数据结构
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
    datastartmarker = print_byte1(0) * 16
    datastartmarkerlength = len(datastartmarker)

    if len(sys.argv) > 1:
        filename = sys.argv[1]
        if filename.lower().endswith('.csv'):
            # 读取和处理CSV文件
            with open(filename, 'r', encoding='utf-8') as f:
                mycsv = csv.reader(f)
                for row in tqdm(mycsv, desc="Processing CSV"):
                    therest = ''
                    if len(row) == 10:  # 城市级数据
                        dbtype = 'city'
                        for i in range(2, 6):
                            tokens[row[i]] = 0
                        latlongs[row[6]] = 0
                        latlongs[row[7]] = 0
                        tokens[row[8]] = 0
                        cities["|".join(row[2:9])] = 0
                        therest = "|".join([row[2], row[4], row[5], row[6], row[7], row[8]])
                    else:  # 国家级数据
                        dbtype = 'country'
                        countries[row[2]] = row[3]
                        therest = row[2]
                    
                    # 处理IP范围
                    fromip = ip2no(row[0])
                    toip = ip2no(row[1])
                    startip = ipaddress.IPv4Address(fromip)
                    endip = ipaddress.IPv4Address(toip)
                    ar = [ipaddr for ipaddr in ipaddress.summarize_address_range(startip, endip)]
                    ar1 = sorted(str(cidr) for cidr in ar)
                    for cidr in ar1:
                        cidrdata.append(f'"{cidr}",{therest}')

            # 处理CIDR数据
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

            # 构建数据部分
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

            # 处理国家或城市数据
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

            # 更新B树
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

            # 遍历B树并构建搜索树
            logging.info("Traversing B-tree")
            travtree(btree, 0, '')

            # 计算节点总数和偏移量
            totalnodes = sum(len(level) for level in data.values())
            offsetnodes = {i: sum(len(data[j]) for j in range(i+1)) for i in range(len(data))}

            # 写入MMDB文件
            logging.info("Writing to file")
            filename2 = filename + '.MMDB'
            with open(filename2, 'wb') as f:
                # 写入搜索树节点
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

                # 写入数据开始标记和数据部分
                f.write(datastartmarker)
                f.write(datasection)
                f.write(binascii.unhexlify(b'ABCDEF4D61784D696E642E636F6D'))

                # 写入元数据
                controlbyte = MAP_TYPE | 9
                f.write(print_byte(controlbyte))

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

                # 写入每个元数据字段
                for key, value in metadata.items():
                    f.write(print_byte(STRING_TYPE | len(key)) + key.encode())
                    if isinstance(value, int):
                        if value < 256:
                            f.write(print_byte(UINT16_TYPE | 1) + print_byte(value))
                        elif value < 65536:
                            f.write(print_byte(UINT16_TYPE | 2) + print_uint(value))
                        elif value < 4294967296:
                            f.write(print_byte(UINT32_TYPE | 4) + print_uint(value))
                        else:
                            f.write(print_byte(EXTENDED_TYPE | 8) + print_byte(UINT64_TYPE) + print_uint(value))
                    elif isinstance(value, str):
                        f.write(print_byte(STRING_TYPE | len(value)) + value.encode())
                    elif isinstance(value, dict):
                        f.write(print_byte(MAP_TYPE | len(value)))
                        for sub_key, sub_value in value.items():
                            f.write(print_byte(STRING_TYPE | len(sub_key)) + sub_key.encode())
                            f.write(print_byte(STRING_TYPE | len(sub_value)) + sub_value.encode())
                    elif isinstance(value, list):
                        f.write(print_byte(EXTENDED_TYPE | 1) + print_byte(ARRAY_TYPE))
                        for item in value:
                            f.write(print_byte(STRING_TYPE | len(item)) + item.encode())

            logging.info(f"You have successfully converted {filename} to {filename2}.")
            print(f"You can now use {filename2} with any MaxMind API which supports the GeoLite2 format.\n")

        else:
            raise Exception('Only .csv files are accepted.')
    else:
        print("Usage: python3 convert.py <IP2Location LITE DB1 or DB11 CSV file>\n")
        raise Exception('Please enter a filename.')

if __name__ == '__main__':
    main()
