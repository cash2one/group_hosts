# -*-coding:utf8-*-
import MySQLdb
import MySQLdb.cursors
import time

from config import log, machine_db, portal_db, creator, data_grp


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

    for leaf in leafnodes:
        path = []
        tmp_dict = {}
        tmp_hostid = []

        cursor = mm_conn.execute("select machine_IP from instance i join namespace_machine_relation n on i.machine_id=n.machine_id where n.namespace = %s", (leaf[0],))
        machine = cursor.fetchall()
        cursor and cursor.close()

        for machine_ip in machine:
            cursor = fp_conn.execute("select id from host where ip=%s", (machine_ip[0],))
            exist = cursor.fetchone()
            if exist:
                tmp_hostid.append(exist[0])

        if not len(tmp_hostid):
            continue

        get_namespace(mm_conn, leaf[0], path)

        tmp_dict['hostid'] = tmp_hostid
        tmp_dict['namespace'] = '/'.join(path)

        namespace_hostid.append(tmp_dict)

    return namespace_hostid


def get_grp_id(conn, name):
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


def get_hostname(conn, host_id):
    cursor = conn.execute("select hostname from host where id=%s", (host_id,))
    hostname = cursor.fetchone()
    cursor and cursor.close()
    if hostname:
        return hostname[0]
    return None


def get_all_host(conn):
    cursor = conn.execute("select id from host where ip !=''")
    hostids = cursor.fetchall()
    cursor and cursor.close()
    return hostids


def del_from_host(conn, host_id):
    cursor = conn.execute("select hostname from host where id=%s", (host_id,))
    name = cursor.fetchone()
    cursor and cursor.close()

    cursor = conn.execute("delete from host where id=%s", (host_id,))
    conn.commit()
    cursor and cursor.close()

    if name:
        log.info("delete from host:hostname:%s" % name[0])


def get_all_group_hosts(conn):
    """
    read all host in group except group:base
    :param conn:
    :return:
    """
    sql = "select distinct host_id from grp_host where grp_id in (select id from grp where grp_name not in ('base'))"
    cursor = conn.execute(sql)
    result = cursor.fetchall()
    cursor and cursor.close()
    return result


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


def del_grp_host(conn, grp_name, grp_id, host_id):
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
            # 从machineTool中更新命名空间以及其对应的机器信息
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
                grp_id = get_grp_id(fp_conn, namespace)
                if not grp_id:
                    grp_id = add_group(fp_conn, namespace)
                    if grp_id:
                        for h in hostids:
                            add_host(fp_conn, namespace, grp_id, h)

                else:
                    result = get_hostids(fp_conn, grp_id)
                    group_hosts = [x[0] for x in result]
                    for h in group_hosts:
                        if h not in hostids:
                            del_grp_host(fp_conn, namespace, grp_id, h)

                    for h in hostids:
                        if h not in group_hosts:
                            add_host(fp_conn, namespace, grp_id, h)

            mm_conn._conn.close()
            fp_conn._conn.close()

        flag = False
        time.sleep(600)







