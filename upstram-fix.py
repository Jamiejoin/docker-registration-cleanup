#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 17/11/2017 2:26 PM
# @Author  : Fish
# @Site    : 
# @File    : upstream_fix.py
# @Software: PyCharm
# pip3 install python-etcd  requests

import requests
import etcd
import random

mURL = 'http://marathon.com:8080/v2/apps'
etcdHost='etcd.aws.com'
etcdPort=80
etcdNode =['dev','qa']
headers = {"Content-Type": "application/json"
           }
hosts = {
    'a01.mesos.slave.dev.cd.com': '10.28.3.138',
    'a02.mesos.slave.dev.cd.com': '10.28.3.144',
    'a03.mesos.slave.dev.cd.com': '10.28.3.149',
    'a04.mesos.slave.dev.cd.com': '10.28.3.139',
    'a01.mesos.slave.qa.cd.com': '10.28.3.137',
    'a02.mesos.slave.qa.cd.com': '10.28.3.135',
    'a03.mesos.slave.qa.cd.com': '10.28.3.132',
    'a04.mesos.slave.qa.cd.com': '10.28.3.148',
    'a05.mesos.slave.qa.cd.com': '10.28.3.134'
}


## 获取marathon 所有 任务列表
def getMarathonApps():
    results = list()
    reps = requests.get(mURL, headers=headers)
    apps = reps.json()['apps']

    for id in apps:
        results.append(id['id'].lstrip('/'))
    return results


## 根据任务ID获取task信息
def getMarathonAppIP(app):
    # appURL = 'http://marathon.cd.com:8080/v2/apps/' + app + '/tasks'
    appURL = mURL + '/' + app + '/tasks'
    reps = requests.get(appURL, headers=headers)
    IPList = {
        'host': list(),
        'IPPort': list()
    }
    if len(reps.json()['tasks']) > 0:
        for IP in reps.json()['tasks']:
            if len(IP) > 0 and IP['state'] == 'TASK_RUNNING':
                IPList['host'].append(IP['host'])
                IPList['IPPort'].append(IP['ports'][0])

    if len(IPList['host']) > 0:
        return IPList
    else:
        return None


## 根据域名查找etcd中upstream
## 返回upstream 列表
def getEtcdUpstream(node, domainName):
    if node and domainName:
        key = '/' + node + '/' + domainName

    client = etcd.Client(host=etcdHost, port=etcdPort)
    try:
        upstreams = client.read(key, recursive=True)
        res = list()
        for ups in upstreams.children:
            res.append(ups.value)
        return res
    except etcd.EtcdKeyNotFound:
        return None


def checkIPPort():
    pass


def writeEtcdkey(node, domainName, upstream=['127.0.0.1:51777']):
    client = etcd.Client(host=etcdHost, port=etcdPort)
    if node and domainName and upstream:
        for ups in upstream:
            key = '/' + node + '/' + domainName + '/' + domainName + '-' + 'admin' + '-' + str(random.randint(100,10000))
            print('正在写入etcd key : %s'%(key))
            client.write(key, ups)

## 删除etcd key
def deleteEtcdkey(node, domainName, value):
    if node and domainName and value :
        key = '/' + node + '/' + domainName
    client = etcd.Client(host=etcdHost, port=etcdPort)
    upstreams = client.read(key, recursive=True)
    for ups in upstreams.children:
        print(ups)
        if ups.value == value:
            print('删除etcd key: %s' % (ups.key))
            client.delete(ups.key)


## 清理 节点
def clearNode(node,app,appinfo):
    if appinfo and node and app:
        IP_Port = hosts[appinfo['host'][0]] + ":" + str(appinfo['IPPort'][0])
        print(IP_Port)

        ups = getEtcdUpstream(node, app)
        if ups != None:
            print("etcd ups 是: %s" % (ups))
            for upstream in ups:
                if upstream != IP_Port and upstream !=None:
                    print('upstream: %s 与 marathon: %s 不一致需要进行删除 \n' % (upstream, IP_Port))
                    deleteEtcdkey(node, app, upstream)
                else:
                    print('upstream: %s 与 marathon: %s 一致无需删除 \n' % (upstream, IP_Port))
        else:
            print('upstream: %s 在etcd /%s 中不存在\n' %(IP_Port,node))

## 为防止串线,更新etcd,即marathon中存在且etcd中不存在的项
def updateNode(node,app,appinfo):
    if appinfo and node and app:
        IP_Port = hosts[appinfo['host'][0]] + ":" + str(appinfo['IPPort'][0])
        domainName=str(appinfo['host'][0])
        print(IP_Port)

        ups = getEtcdUpstream(node, app)
        if ups==None and node[0:2]==domainName[0:2]:
            print('marathon upstream  在etcd中不存在，手动添加 %s    %s \n' %(domainName,IP_Port))
            writeEtcdkey(node,domainName,upstream=list(IP_Port))
        else:
            print("etcd 中 upstream 非空无需手动添加 \n")



if __name__ == '__main__':
    apps = getMarathonApps()
    # print(apps)
    for app in apps:
        print(app)
        appinfo=getMarathonAppIP(app)
        print(appinfo)
        for node in etcdNode:
            clearNode(node=node,app=app, appinfo=appinfo)
            updateNode(node=node,app=app,appinfo=appinfo)

