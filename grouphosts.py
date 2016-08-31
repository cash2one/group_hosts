# -*-coding:utf8-*-
import MySQLdb
import MySQLdb.cursors
import time
import json
import socket
import itertools

from config import log, machine_db, portal_db, creator, transfer


def connect_db(host, port, user, password, db):
    try:
        conn = MySQLdb.connect(
            host=host,
            port=port,
            user=user,
            passwd=password,
            db=db,
            use_unicode=True,
            charset="utf8")
        return conn
    except Exception, e:
        log.error("Fatal: connect db fail:%s" % e)
        return None


class DB(object):

    def __init__(self, host, port, user, password, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self._conn = connect_db(host, port, user, password, db)

    def connect(self):
        self._conn = connect_db(self.host, self.port, self.user, self.password, self.db)
        return self._conn

    def execute(self, *a, **kw):
        cursor = kw.pop('cursor', None)
        try:
            cursor = cursor or self._conn.cursor()
            cursor.execute(*a, **kw)
        except (AttributeError, MySQLdb.OperationalError):
            self._conn and self._conn.close()
            #self.connect()
            self.reconnect()
            cursor = self._conn.cursor()
            cursor.execute(*a, **kw)
        return cursor

    def commit(self):
        if self._conn:
            try:
                self._conn.commit()
            except MySQLdb.OperationalError:
                self._conn and self._conn.close()
                #self.connect()
                self.reconnect()
                self._conn and self._conn.commit()

    def rollback(self):
        if self._conn:
            try:
                self._conn.rollback()
            except MySQLdb.OperationalError:
                self._conn and self._conn.close()
                self.connect()
                self._conn and self._conn.rollback()

    def reconnect(self, number=14400, stime=60):
        num = 0
        while num <= number:
            self.connect()
            if self._conn:
                break
            num += 1
            time.sleep(stime)


# class RPCClient(object):
#     def __init__(self, addr, codec=json):
#         self._socket = socket.create_connection(addr)
#         self._id_iter = itertools.count()
#         self._codec = codec
#
#     def _message(self, name, *params):
#         return dict(id=self._id_iter.next(),
#                     params=list(params),
#                     method=name)
#
#     def call(self, name, *params):
#         req = self._message(name, *params)
#         id = req.get('id')
#
#         mesg = self._codec.dumps(req)
#         self._socket.sendall(mesg)
#
#         # This will actually have to loop if resp is bigger
#         resp = self._socket.recv(4096)
#         resp = self._codec.loads(resp)
#
#         if resp.get('id') != id:
#             raise Exception("expected id=%s, received id=%s: %s"
#                             % (id, resp.get('id'), resp.get('error')))
#
#         if resp.get('error') is not None:
#             raise Exception(resp.get('error'))
#
#         return resp.get('result')
#
#     def close(self):
#         self._socket.close()


# def push_data_to_transfer(endpoint):
#     addr = transfer.get('addr')
#     port = transfer.get('port')
#     try:
#         rpc = RPCClient((addr, port))
#     except Exception, e:
#         log.error("rpc error:%s" % e)
#         return
#     metric = dict(endpoint=endpoint, metric='no_agent', value=1, step=60,
#                   counterType='GAUGE', tags='no_agent', timestamp=int(time.time()))
#
#     result = rpc.call("Transfer.Update", [metric])
#
#     if result['Message'] == 'ok':
#         log.info("push no_agent metric to transfer success,host:%s" % endpoint)
#     else:
#         log.error("push no_agent metric to transfer error,host:%s" % endpoint)


# def add_no_agent_host(conn, hostip):
#     cursor = conn.execute("insert into host(hostname) value(%s) ON DUPLICATE KEY update hostname=%s", (hostip, hostip))
#     host_id = cursor.lastrowid
#     cursor and cursor.close()
#     conn.commit()
#     return host_id
#
#
# def delete_no_agent_host(conn, host_id):
#     cursor = conn.execute("select id from host where ip =(select hostname from host where id = %s)", (host_id,))
#     exist = cursor.fetchone()
#     cursor and cursor.close()
#     if exist:
#         cursor = conn.execute("delete from host where id=%s", (host_id,))
#         conn.commit()
#         cursor and cursor.close()
#
#
# def get_no_agent_hostids(conn):
#     grp_id = get_groupid(conn, 'no_agent')
#     if grp_id is not None:
#         hostids = get_hostids(conn, grp_id)
#         return hostids
#     return None


def get_host_by_hostname(conn, name):
    cursor = conn.execute('''select id from host where hostname=%s''', (name,))
    host_id = cursor.fetchone()
    cursor and cursor.close()
    if host_id is not None:
        return host_id[0]
    return None


def get_group_host(conn, grp_id, host_id):
    cursor = conn.execute("select * from grp_host where grp_id=%s and host_id=%s", (grp_id, host_id))
    exist = cursor.fetchone()
    cursor and cursor.close()
    return exist


# def no_agent_alarm(conn, no_agent_hosts, group="no_agent"):
#     grp_id = get_groupid(conn, group)
#     if not grp_id:
#         grp_id = add_group(conn, group)
#
#     if grp_id is None:
#         log.error("insert group no_agent into table grp error!")
#         return
#
#     for hostip in no_agent_hosts:
#         host_id = get_host_by_hostname(conn, hostip)
#         if host_id is None:
#             host_id = add_no_agent_host(conn, hostip)
#
#         if host_id is None:
#             log.error("insert no_agent_host:%s into table host error!" % hostip)
#             continue
#
#         exist = get_group_host(conn, grp_id, host_id)
#         if not exist:
#             add_host(conn, group, grp_id, host_id)
#
#         #push_data_to_transfer(hostip)


def get_namespace(mm_conn, id, path):
    cursor = mm_conn.execute("select des,parent from namespace where namespace_id=%s", (id,))
    result = cursor.fetchone()
    cursor and cursor.close()

    if result:
        path.insert(0, result[0])
        get_namespace(mm_conn, result[1], path)


def get_hostid_by_namespace(mm_conn, fp_conn):
    namespace_hostid = []
    cursor = mm_conn.execute("select namespace_id from namespace where type = 'leaf' ")

    leafnodes = cursor.fetchall()
    cursor and cursor.close()
    # no_agent_hosts = set()

    for leaf in leafnodes:
        path = []
        tmp_dict = {}
        tmp_hostid = []

        cursor = mm_conn.execute("select machine_IP from instance i join namespace_machine_relation n "
                                  "on i.machine_id=n.machine_id where n.namespace = %s", (leaf[0],))
        machine = cursor.fetchall()
        cursor and cursor.close()

        for machine_ip in machine:
            cursor = fp_conn.execute("select id from host where ip=%s", (machine_ip[0],))
            exist = cursor.fetchone()
            if exist:
                tmp_hostid.append(exist[0])
            # else:
            #     no_agent_hosts.add(machine_ip[0])

        if not len(tmp_hostid):
            continue

        get_namespace(mm_conn, leaf[0], path)

        tmp_dict['hostid'] = tmp_hostid
        tmp_dict['namespace'] = '/'.join(path)

        namespace_hostid.append(tmp_dict)

    #no_agent_alarm(fp_conn, no_agent_hosts)
    return namespace_hostid


def get_groupid(conn, name):
    cursor = conn.execute("select id from grp where grp_name=%s", (name,))
    grp_id = cursor.fetchone()
    cursor and cursor.close()
    if grp_id is not None:
        return grp_id[0]
    return None


def get_hostids(conn, grp_id):
    cursor = conn.execute("select host_id from grp_host where grp_id=%s", (grp_id,))
    hostids = cursor.fetchall()
    cursor and cursor.close()
    return hostids


def get_hostip(conn, host_id):
    cursor = conn.execute("select ip from host where id=%s", (host_id,))
    ip = cursor.fetchone()
    cursor and cursor.close()
    if ip:
        return ip[0]
    return None


def get_all_host(conn):
    cursor = conn.execute("select id from host")
    hostids = cursor.fetchall()
    cursor and cursor.close()
    return  hostids


def add_group(conn, group):
    name = creator
    if not name:
        name = "ops"

    cursor = conn.execute("insert into grp(grp_name,create_user) values(%s,%s)", (group, name))
    grp_id = cursor.lastrowid
    conn.commit()
    cursor and cursor.close()
    if grp_id:
        log.info("insert into table grp: grp_name:%s,creator:%s" % (group, name))
    else:
        log.error("insert into table grp fail: grp_name:%s,creator:%s" % (group, name))
    return grp_id


def update_group(conn, group):
    name = creator
    if not name:
        name = "ops"

    cursor = conn.execute("update grp set create_user=%s where grp_name=%s", (name, group))
    conn.commit()
    cursor and cursor.close()
    log.info("update table grp: grp_name:%s,creator:%s" % (group, name))


def del_group(conn, name):
    cursor = conn.execute("delete from grp where grp_name=%s", (name, ))
    conn.commit()
    cursor and cursor.close()
    log.info("delete from table grp:grp_name:%s" % (name,))


def add_host(conn, grp_name, grp_id, host_id):
    cursor = conn.execute("insert into grp_host(grp_id,host_id) values(%s,%s)", (grp_id, host_id))
    id = cursor.lastrowid
    conn.commit()
    cursor and cursor.close()

    ip = get_hostip(conn, host_id)

    if id is not None:
        log.info("insert into table grp_host:grp_name:%s,host_ip:%s,host_id:%s" % (grp_name, ip, host_id))
    else:
        log.error("insert into table grp_host fail: grp_name:%s,host_ip:%s,host_id:%s" % (grp_name, ip, host_id))


def del_host(conn, grp_name, grp_id, host_id):
    cursor = conn.execute("delete from grp_host where grp_id=%s and host_id=%s", (grp_id, host_id))
    conn.commit()
    cursor and cursor.close()

    ip = get_hostip(conn, host_id)
    log.info("delete from table grp_host:grp_name:%s,host_ip:%s,host_id:%s" % (grp_name, ip, host_id))


if __name__ == '__main__':
    flag = True
    while True:
        mm_conn = DB(machine_db.get('host'), machine_db.get('port'), machine_db.get('user'),
                     machine_db.get('password'), machine_db.get('db'))

        fp_conn = DB(portal_db.get('host'), portal_db.get('port'), portal_db.get('user'),
                     portal_db.get('password'), portal_db.get('db'))

        if  mm_conn._conn and  fp_conn._conn:

            # # 更新无agent组信息,删除已成功部署agent的机器
            # hostids = get_no_agent_hostids(fp_conn)
            # if hostids:
            #     for id in hostids:
            #         delete_no_agent_host(fp_conn, id[0])

            # 从cmdb中更新命名空间以及其对应的机器信息
            namespace_hostid = get_hostid_by_namespace(mm_conn, fp_conn)

            if not namespace_hostid:
                log.error("get namespace and host from machine db fail")
                continue


            # 只在第一次执行时更新group的create_user
            if flag:
                for r in namespace_hostid:
                    namespace = r['namespace']
                    update_group(fp_conn, namespace)

            for record in namespace_hostid:
                namespace = record['namespace']
                hostids = record['hostid']

                # 为每个namespace都建立一个group,写入portal
                grp_id = get_groupid(fp_conn, namespace)
                if not grp_id:
                    grp_id = add_group(fp_conn, namespace)
                    if grp_id:
                        for h in hostids:
                            add_host(fp_conn, namespace, grp_id, h)

                else:
                    result = get_hostids(fp_conn, grp_id)
                    grouphosts = []
                    if result:
                        for x in result:
                            grouphosts.append(x[0])

                        for h in grouphosts:
                            if h not in hostids:
                                del_host(fp_conn, namespace, grp_id, h)

                        for h in hostids:
                            if h not in grouphosts:
                                add_host(fp_conn, namespace, grp_id, h)
                    else:
                        for h in hostids:
                            add_host(fp_conn, namespace, grp_id, h)

            # 将所有机器都放入base组,作为基础监控组
            base_id = get_groupid(fp_conn, "base")
            if base_id is None:
                base_id = add_group(fp_conn, "base")
            else:
                if flag:
                    update_group(fp_conn, "base")

            all_hosts = []
            result = get_all_host(fp_conn)
            if result:
                for x in result:
                    all_hosts.append(x[0])

            result = get_hostids(fp_conn, base_id)
            base_hosts = []
            if result:
                for x in result:
                    base_hosts.append(x[0])

            if not base_hosts:
                for h in all_hosts:
                    add_host(fp_conn, "base", base_id, h[0])
            else:
                for h in base_hosts:
                    if h not in all_hosts:
                        del_host(fp_conn, "base", base_id, h)

                for h in all_hosts:
                    if h not in base_hosts:
                        add_host(fp_conn, "base", base_id, h)

            mm_conn._conn.close()
            fp_conn._conn.close()

        flag = False
        time.sleep(600)







