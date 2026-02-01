import datetime
import json
import time
from dataclasses import dataclass
from typing import List

import requests
import urllib3
from dacite import from_dict
from paho.mqtt import client as mqtt

urllib3.disable_warnings()

env = json.load(open('env.json'))

token = env["TOKEN"]
secret = env["SECRET"]
address = env["ADDRESS"]
node = env["NODE"]
unique_id = env["unique_id"]
object_id = env["object_id"]
node_name =  env["node_name"]

broker = env["BROKER"]
port = env["PORT"]
client_id = env["CLIENTID"]


@dataclass
class VmData:
    name: str
    vmid: int
    status: str
    cpu: float
    mem: int
    maxmem: int


@dataclass
class VM:
    data: VmData


@dataclass
class Memory:
    used: float
    free: float
    total: float


@dataclass
class NodeData:
    loadavg: List[str]
    wait: float
    uptime: int
    memory: Memory
    cpu: float


@dataclass
class Node:
    data: NodeData


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT broker.")
        else:
            print(f"Failed to connect to MQTT broker, return code: {rc}")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def set_node_status(client: mqtt.Client, node_node: Node):
    topic = f"homeassistant/sensor/proxmox_node/{node}/state"
    client.publish(topic, "ON")
    node_data = node_node.data
    topic = f"homeassistant/sensor/proxmox_node/{node}/attr"
    payload = {
        "loadavg": node_data.loadavg,
        "wait": round(node_data.wait * 100, 2),
        "uptime": str(datetime.timedelta(seconds=node_data.uptime)),
        "memory_total": round(node_data.memory.total / 1024 / 1024, 1),
        "memory_usage": round(node_data.memory.free / node_data.memory.total * 100, 1),
        "cpu": round(node_data.cpu * 100, 1)
    }
    client.publish(topic, json.dumps(payload))


def set_vm_status(client: mqtt.Client, vm_id: str, status: str):
    topic = f"homeassistant/switch/proxmox_vm/{vm_id}/state"
    result = client.publish(topic, status)
    vm_status = result[0]
    if vm_status != 0:
        print(f'Failed to set state to topic {topic}')


def set_vm_sensors(client: mqtt.Client, vm: VM):
    vm_data = vm.data
    topic = f"homeassistant/sensor/proxmox_vm_memory_total/{vm_data.vmid}/state"
    client.publish(topic, round(vm_data.maxmem / 1024 / 1024, 1))
    topic = f"homeassistant/sensor/proxmox_vm_memory_usage/{vm_data.vmid}/state"
    client.publish(topic, round(vm_data.mem / vm_data.maxmem * 100, 1))
    topic = f"homeassistant/sensor/proxmox_vm_cpu/{vm_data.vmid}/state"
    client.publish(topic, round(vm_data.cpu * 100, 1))


def bootstrap_node(client: mqtt.Client):
    topic = f"homeassistant/sensor/proxmox_node/{node}/config"
    payload = '''{{
        "unique_id": "{unique_id}",
        "object_id": "{object_id}",
        "name": "{node_name}",
        "icon": "mdi:server",
        "state_topic": "homeassistant/sensor/proxmox_node/{node}/state",
        "json_attributes_topic": "homeassistant/sensor/proxmox_node/{node}/attr",
        "json_attributes_template": "{{{{ value_json | tojson }}}}"
    }}'''.format(unique_id=unique_id, object_id=object_id, node_name=node_name, node=node)
    client.publish(topic, payload, retain=True)


def bootstrap_vm(client: mqtt.Client, vm: VM):
    vm_data = vm.data
    # VM Status
    topic = f"homeassistant/switch/proxmox_vm/{vm_data.vmid}/config"
    name = f"{vm_data.name.upper()} ({vm_data.vmid})"
    payload = f'''{{
        "unique_id": "proxmox_vm_{vm_data.vmid}",
        "object_id": "proxmox_vm_{vm_data.vmid}",
        "device_class": "switch",
        "name": "{name}",
        "command_topic": "homeassistant/switch/proxmox_vm/{vm_data.vmid}/set",
        "state_topic": "homeassistant/switch/proxmox_vm/{vm_data.vmid}/state"
    }}'''
    client.publish(topic, payload, retain=True)
    # VM Memory Total
    name = f"VM Total Memory"
    topic = f"homeassistant/sensor/proxmox_vm_memory_total/{vm_data.vmid}/config"
    payload = f'''{{
        "unique_id": "proxmox_vm_{vm_data.vmid}_memory_total",
        "object_id": "proxmox_vm_{vm_data.vmid}_memory_total",
        "device_class": "data_size",
        "unit_of_measurement": "MB",
        "name": "{name}",
        "icon": "mdi:server",
        "state_topic": "homeassistant/sensor/proxmox_vm_memory_total/{vm_data.vmid}/state"
    }}'''
    client.publish(topic, payload, retain=True)
    # VM Memory Usage
    name = f"VM Memory Usage"
    topic = f"homeassistant/sensor/proxmox_vm_memory_usage/{vm_data.vmid}/config"
    payload = f'''{{
        "unique_id": "proxmox_vm_{vm_data.vmid}_memory_usage",
        "object_id": "proxmox_vm_{vm_data.vmid}_memory_usage",
        "device_class": "battery",
        "unit_of_measurement": "%",
        "name": "{name}",
        "icon": "mdi:memory",
        "state_topic": "homeassistant/sensor/proxmox_vm_memory_usage/{vm_data.vmid}/state"
    }}'''
    client.publish(topic, payload, retain=True)
    # VM CPU
    name = f"VM CPU Usage"
    topic = f"homeassistant/sensor/proxmox_vm_cpu/{vm_data.vmid}/config"
    payload = f'''{{
        "unique_id": "proxmox_vm_{vm_data.vmid}_cpu",
        "object_id": "proxmox_vm_{vm_data.vmid}_cpu",
        "device_class": "battery",
        "unit_of_measurement": "%",
        "name": "{name}",
        "icon": "mdi:cpu-64-bit",
        "state_topic": "homeassistant/sensor/proxmox_vm_cpu/{vm_data.vmid}/state"
    }}'''
    client.publish(topic, payload, retain=True)


class Proxmox:
    def __init__(self):
        self.token = token
        self.secret = secret
        self.address = address

    def node_status(self):
        url = f'{self.address}api2/json/nodes/{node}/status'
        result = self.call_api(url, 'GET')
        return from_dict(Node, json.loads(result.content))

    def vm_status(self, vm_id: str) -> VM:
        url = f'{self.address}api2/json/nodes/{node}/{self.vm_type(vm_id)}/{vm_id}/status/current'
        result = self.call_api(url, 'GET')
        if result.status_code == 200:
            return from_dict(VM, json.loads(result.content))
        return None  # type: ignore

    def vm_state(self, vm_id: str):
        result = self.vm_status(vm_id)
        if result is None:
            raise RuntimeError("VM status returned no data!")
        if result.data.status == "running":
            return "ON"
        if result.data.status == "stopped":
            return "OFF"

    def auth(self):
        return {"Authorization": f"PVEAPIToken={self.token}={self.secret}"}

    def vm_type(self, vm_id: str):
        url = f'{self.address}api2/json/nodes/{node}/qemu'
        result = self.call_api(url, 'GET')
        qemu_list = json.loads(result.content)["data"]
        if [element for element in qemu_list if str(element['vmid']) == str(vm_id)]:
            return "qemu"
        url = f'{self.address}api2/json/nodes/{node}/lxc'
        result = self.call_api(url, 'GET')

        lxc_list = json.loads(result.content)["data"]
        if [element for element in lxc_list if str(element['vmid']) == str(vm_id)]:
            return "lxc"

    def start(self, vm_id: str):
        url = f'{self.address}api2/json/nodes/{node}/{self.vm_type(vm_id)}/{vm_id}/status/start'
        self.call_api(url, 'POST')
        print(f'VM {vm_id} turned on.')

    def shutdown(self, vm_id: str):
        url = f'{self.address}api2/json/nodes/{node}/{self.vm_type(vm_id)}/{vm_id}/status/shutdown'
        self.call_api(url, 'POST')
        print(f'VM {vm_id} turned off.')

    def call_api(self, url: str, method: str) -> requests.Response:
        result = requests.Response
        if method == 'GET':
            try:
                result = requests.get(url, headers=self.auth(), verify=False)
                return result
            except Exception as e:
                print(e)
        if method == 'POST':
            try:
                result = requests.post(url, headers=self.auth(), verify=False)
                return result
            except Exception as e:
                print(e)
        return result  # type: ignore


def on_message(client: mqtt.Client, userdata, message: mqtt.MQTTMessage):
    msg = str(message.payload.decode("utf-8"))
    if msg == "ON":
        proxmox.start(vm_id(message.topic))
    if msg == "OFF":
        proxmox.shutdown(vm_id(message.topic))
    set_vm_status(client, vm_id(message.topic), msg)


def vm_id(topic: str):
    return topic.split('/')[-2]


vms = env["VMS"]
proxmox = Proxmox()

client = connect_mqtt()
client.loop_start()

bootstrap_node(client)

for vm in vms:
    bootstrap_vm(client, proxmox.vm_status(vm))

client.subscribe("homeassistant/switch/proxmox_vm/+/set")
client.on_message = on_message

while True:
    set_node_status(client, proxmox.node_status())
    for vm in vms:
        set_vm_status(client, vm, proxmox.vm_state(vm))  # type: ignore
        set_vm_sensors(client, proxmox.vm_status(vm))
    time.sleep(60)
