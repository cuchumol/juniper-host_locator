from lib.validate_ip_address import validate_ip_address

from jnpr.junos import Device
from jnpr.junos.op.arp import ArpTable

from jnpr.junos.factory.factory_loader import FactoryLoader
import yaml
import yamlordereddictloader

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
from datetime import datetime



ip_address = ""

list_pub_switch = []

list_gw = []


map_rack = {}

USER=""
PASS=""

myYAML = """
---
EtherSwTable:
  rpc: get-interface-ethernet-switching-table
  item: ethernet-switching-table/mac-table-entry[mac-type='Learn']
  key: mac-address
  view: EtherSwView
EtherSwView:
  fields:
    vlan_name: mac-vlan
    mac: mac-address
    mac_type: mac-type
    mac_age: mac-age
    interface: mac-interfaces-list/mac-interfaces
"""


globals().update(FactoryLoader().load(yaml.load(myYAML,Loader=yamlordereddictloader.Loader)))


def arp_parse(host, ipa, event):
    dev = Device(host=host, port = 22, user=USER, password=PASS)
    
    if event.is_set():
        return

    try:
        dev.open()
        arp_table = ArpTable(dev).get()
        dev.close()
    except Exception:
        print(host + " " + "anrichable_error")

    try:
        t = 0
        for i in arp_table.keys():
            if event.is_set():
                return
            if arp_table[i]['ip_address'] == ipa:
                return arp_table[i]['mac_address']
    except Exception:
        print(host, "except arp parsing")


def get_mac(gateway_nodes, ipa, max_workers):
    event = Event()

    mac = None

    start_time = datetime.now()
    
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_list = []

        for node in gateway_nodes:
            future = ex.submit(arp_parse, node, ipa, event)
            future_list.append(future)

        for f in as_completed(future_list):
            if f.result() is not None:
                event.set()
                mac = f.result()
                break
        ex.shutdown(cancel_futures=True)

    print("Time: ", datetime.now() - start_time)

    return mac
                

def ethernetswitching_parse(host, mac, event):
    if event.is_set():
        print("cancel task ", host)
        return
    
    dev = Device(host=host, port = 22, user=USER, password=PASS)

    try:
        dev.open()
        eth_table = EtherSwTable(dev).get()        
        dev.close()
   
    except Exception:
        print(host + " " + "anrichable_error")

    try:
        for i in eth_table.keys():
            if event.is_set():
                #print(host + " unset task")
                return
            
            if "ae" not in eth_table[i]['interface'] and eth_table[i]['mac'] == mac:
                return host, eth_table[i]['interface']

    except Exception:
        print(host, " except ethernet switching parsing")


def get_port(pub_switches, mac, max_workers):
    event = Event()
    host, interface = None, None

    start_time = datetime.now()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_list = []

        for node in pub_switches:
            future = ex.submit(ethernetswitching_parse, node, mac, event)
            future_list.append(future)

        for f in as_completed(future_list):
            if f.result() is not None:
                event.set()
                host, interface = f.result()
                break
        ex.shutdown(cancel_futures=True)

    print("Time: ", datetime.now() - start_time)

    return host, interface

if __name__ == "__main__":

    if validate_ip_address(ip_address):
        mac = get_mac(list_gw, ip_address, 3) # 3 max_workers
    print(mac)


    if mac is not None:
        host, interface = get_port(list_pub_switch, mac, 10) # 10 max_workers

    if host is not None and interface is not None:
        print(host, map_rack[host], mac, interface)    
