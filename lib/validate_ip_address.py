import ipaddress

def validate_ip_address(address):
    try:
        ip = ipaddress.ip_address(address)
        return True
    except ValueError:
        return False