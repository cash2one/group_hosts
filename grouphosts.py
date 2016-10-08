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

#
# def get_host_by_hostname(conn, name):
#     cursor = conn.execute('''select id from host where hostname=%s''', (name,))
#     host_id = cursor.fetchone()
#     cursor and cursor.close()
#     if host_id is not None:
#         return host_id[0]
#     return None


# def get_group_host(conn, grp_id, host_id):
#     cursor = conn.execute("select * from grp_host where grp_id=%s and host_id=%s", (grp_id, host_id))
#     exist = cursor.fetchone()
#     cursor and cursor.close()
#     return exist


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

            machine_hosts = set()
            for record in namespace_hostid:
                namespace = record['namespace']
                hostids = record['hostid']

                # 统计机器管理中同步过来的所有机器
                machine_hosts.update(hostids)

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

            # 删除host表中冗余的机器:既不在机器管理中,也不在group中(除base组,base组中的机器就是host表中的所有机器)
            # result = get_all_group_hosts(fp_conn)
            # all_group_hosts = [x[0] for x in result]
            #
            # machine_hosts.update(all_group_hosts)

            # result = get_all_host(fp_conn)
            # all_hosts = [x[0] for x in result]

            # #还需要判断该机器无数据上传才删除???
            # for h in all_hosts:
            #     if h not in machine_hosts:
            #         del_from_host(fp_conn, h)

            # 获取所有机器列表,将data组的机器加到data组,hadoop2组机器加入hadoop组,其余都加到base组
            result = get_all_host(fp_conn)
            all_hosts = []
            data_grp_hosts = []
            hadoop2_grp_hosts = []

            for x in result:
                c = 0
                ip = get_hostip(fp_conn, x[0])
                hostname = get_hostname(fp_conn, x[0])
                if ip and ip[:8] in data_grp:
                    c = 1
                    data_grp_hosts.append(x[0])

                if hostname and hostname.startswith("hadoop2-"):
                    c = 1
                    hadoop2_grp_hosts.append(x[0])

                if c == 1:
                    continue

                all_hosts.append(x[0])

            # 放入base组
            base_id = get_grp_id(fp_conn, "base")
            if base_id is None:
                base_id = add_group(fp_conn, "base")
            else:
                if flag:
                    update_group(fp_conn, "base")

            result = get_hostids(fp_conn, base_id)
            base_hosts = [x[0] for x in result]

            for h in base_hosts:
                if h not in all_hosts:
                    del_grp_host(fp_conn, "base", base_id, h)

            for h in all_hosts:
                if h not in base_hosts:
                    add_host(fp_conn, "base", base_id, h)

            #放入data组
            data_id = get_grp_id(fp_conn, "data")
            if data_id is None:
                data_id = add_group(fp_conn, "data")
            else:
                if flag:
                    update_group(fp_conn, "data")

            result = get_hostids(fp_conn, data_id)
            data_hosts = [x[0] for x in result]

            for h in data_grp_hosts:
                if h not in data_hosts:
                    add_host(fp_conn, "data", data_id, h)

            for h in data_hosts:
                if h not in data_grp_hosts:
                    del_grp_host(fp_conn, "data", data_id, h)

            # 放入hadoop组
            hadoop2_id = get_grp_id(fp_conn, "hadoop2")
            if hadoop2_id is None:
                hadoop2_id = add_group(fp_conn, "hadoop2")
            else:
                if flag:
                    update_group(fp_conn, "hadoop2")

            result = get_hostids(fp_conn, hadoop2_id)
            hadoop2_hosts = [x[0] for x in result]

            for h in hadoop2_grp_hosts:
                if h not in hadoop2_hosts:
                    add_host(fp_conn, "hadoop2", hadoop2_id, h)

            for h in hadoop2_hosts:
                if h not in hadoop2_grp_hosts:
                    del_grp_host(fp_conn, "hadoop2", hadoop2_id, h)

            mm_conn._conn.close()
            fp_conn._conn.close()

        flag = False
        time.sleep(600)







