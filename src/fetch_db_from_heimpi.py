import os
import paramiko
from scp import SCPClient
import yaml

def createSSHClient(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


# we assume this code is in /src while data is in /data. Since we do not want to assume a cwd we switch to src and than move one up
path = os.path.dirname(os.path.realpath(__file__))
os.chdir(path)
os.chdir(os.path.pardir)

server_config = yaml.safe_load(open('conf/server.yaml', 'r'))
credentials = yaml.safe_load(open('conf/credentials.yaml', 'r'))

ssh = createSSHClient(server_config['ip'], 
                        server_config['port'], 
                        credentials['user'], 
                        credentials['password'])
scp = SCPClient(ssh.get_transport())

scp.get(remote_path='~/Scripts/FinDat/data/database.csv', local_path='data/database.csv')