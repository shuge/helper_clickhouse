import sys
import traceback
import logging
import functools

import requests


def log_if_code_neq_200(func):
    @functools.wraps(func)
    def wrapper(self, sql, *args, **kwargs):
        r = func(self, sql, *args, **kwargs)
        if r is not None and r.status_code != 200:
            msg = "curl '{url}' -d '{req_body}' \nresp.code:{resp_code} resp.body: {resp_body}".format(
                    url=self.compose_url(),
                req_body=sql,
                resp_code=r.status_code,
                resp_body=r.content,
            )
            self.logger.warn(msg)
        return r
    return wrapper


class ClickhouseProxy(object):
    # 16KB
    QUERY_MAX_SIZE = 16 * 1024

    MAX_RECORDS = 10240

    # https://clickhouse.yandex/docs/en/interfaces/formats/
    FORMATS = (
        "TabSeparated",
        "JSON",
    )

    def __init__(self,
        logger,
        host="127.0.0.1",
        port_http=8123,
        username="default",
        password=None,
        dbn="default",
        timeout=2.0,
        fmt="json",
        nodry=False,
        ):

        self.logger = logger
        self.host = host
        self.port_http = port_http
        self.username = username
        self.password = password
        self.db = dbn
        self.timeout = timeout

        fmt = fmt.upper()
        assert fmt in ClickhouseProxy.FORMATS
        self.fmt = fmt

        self.nodry = nodry

    def compose_url(self):
        if self.username and self.password:
            url = 'http://{host}:{port}/?user={username}&password={password}'.format(
                host=self.host,
                port=self.port_http,
                username=self.username,
                password=self.password,
            )
        else:
            url = 'http://{host}:{port}/'.format(
                host=self.host,
                port=self.port_http,
            )

        return url


    @log_if_code_neq_200
    def query(self, sql, nodry, timeout=None):
        http_timeout = timeout or self.timeout
        url = self.compose_url()
        msg = "curl '%s' -d '%s'" % (url, sql)
        self.logger.debug(msg)

        if not nodry:
            return

        r = requests.post(url=url, data=sql, timeout=http_timeout)
        return r

    def query_parse(self, sql, nodry):
        if self.fmt:
            sql +=" format " + self.fmt

        sql = sql.strip()
        if len(sql) > ClickhouseProxy.QUERY_MAX_SIZE:
            msg = "SQL max length is %d, got %d" % (ClickhouseProxy.QUERY_MAX_SIZE, len(sql))
            self.logger.error(msg)
            return

        r = self.query(sql, nodry)
        if r is None:
            return []

        if r.status_code != 200:
            return []

        if self.fmt == "JSON":
            # NOTICE: create table on single node returns nothing
            if not r.content:
                return []

            # NOTICE: append 'format JSON' into HTTP interface doesn't works as expected, you will get TabSeparated format.
            # TODO: fixed me.
            sql_lower = sql.lower()
            if sql_lower.find("create table") != -1 and sql_lower.find("on cluster") != -1:
                return []

            try:
                body_in_json = r.json()
            except ValueError:
                traceback.print_exc(file=sys.stderr)
                msg = "expected response in JSON, got -%s-" % r.content
                self.logger.error(msg)
                raise

            return ClickhouseProxy.parse_json_resp(body_in_json)
        else:
            return r.content

    @staticmethod
    def parse_json_resp(resp):
        if "data" in resp:
            return resp["data"]

    def show_tables(self):
        sql = "show tables"
        return self.query_parse(sql=sql, nodry=True)

    def desc_table(self, name):
        sql = "desc %s" % name
        return self.query_parse(sql=sql, nodry=True)

    def exist_table(self, name):
        sql = "exists %s" % name
        for row in self.query_parse(sql=sql, nodry=True):
            if row["result"]:
                return True
            break
        return False

    def diff_fields(self, old, new, reservered=None):
        """
        old and new in format

            [
               {"name": "vesion", "type": Uint8},
               ...
            ]
        """
        changes = []

        old_fields = dict()
        for item in old:
            old_fields[item["name"]] = item["type"]

        new_fields = dict()
        for item in new:
            new_fields[item["name"]] = item["type"]

        for item in new:
            if item["name"] not in old_fields:
                changes.append(dict(
                    op="add",
                    name=item["name"],
                    type=item["type"],
                ))
            else:
                old_type = old_fields[item["name"]]
                new_type = item["type"]
                if old_type != new_type:
                    changes.append(dict(
                        op="modify",
                        name=item["name"],
                        type=item["type"],
                    ))

        for item in old:
            if item["name"] not in new_fields and (reservered and item["name"] not in reservered):
                changes.append(dict(
                    op="drop",
                    name=item["name"],
                ))

        return changes

    def alter_table(self, tbl_name, changes, cluster=None):
        tpl = "ALTER TABLE {dbn}.{tbl_name} {on_cluster} {actions}"

        actions = []
        for item in changes:
            if item["op"] == "add":
                action = "ADD COLUMN {name} {type}".format(
                        name=item["name"],
                        type=item["type"],
                        )
                actions.append(action)
            elif item["op"] == "drop":
                action = "DROP COLUMN {name}".format(
                        name=item["name"],
                        )
                actions.append(action)
            if item["op"] == "modify":
                action = "MODIFY COLUMN {name} {type}".format(
                        name=item["name"],
                        type=item["type"],
                        )
                actions.append(action)

        if cluster:
            sql = tpl.format(
               dbn=self.dbn,
               tbl_name=tbl_name,
               actions=", ".join(actions),
               on_cluster="ON CLUSTER %s" % cluster.strip(),
            )
        else:
            sql = tpl.format(
               dbn=self.dbn,
               tbl_name=tbl_name,
               actions=", ".join(actions),
               on_cluster="",
            )

        return self.query_parse(sql=sql, nodry=self.nodry)

    def list_partitions(self):
        sql = 'select partition, table, formatReadableSize(sum(bytes)) AS sum_size from system.parts group by partition,table order by partition asc limit {max_records}'.format(
            max_records=ClickhouseProxy.MAX_RECORDS,
        )

        return self.query_parse(sql=sql, nodry=True)


    def delete_partition(self, partition, table, cluster=None):
        if cluster:
            sql = 'ALTER TABLE {dbn}.{table} on cluster {cluster} DROP PARTITION {partition}'.format(
                dbn=self.dbn,
                table=table,
                cluster=cluster,
                partition=partition,
            )
        else:
            sql = 'ALTER TABLE default.{table} DROP PARTITION {partition}'.format(
                table=table,
                partition=partition,
            )
        return self.query_parse(sql=sql, nodry=self.nodry)

    def list_cluster_nodes(self, cluster=None):
        if cluster:
            sql = "SELECT * FROM system.clusters WHERE cluster='%s'" % cluster
        else:
            sql = 'SELECT * FROM system.clusters' 

        return self.query_parse(sql=sql, nodry=True)



logger = logging.Logger(__name__)
log_hdl_steam = logging.StreamHandler()
log_fmt = logging.Formatter(
    fmt='[%(levelname)s] %(asctime)s %(name)s:%(filename)s:%(lineno)d %(message)s')
log_hdl_steam.setFormatter(fmt=log_fmt)
logger.addHandler(log_hdl_steam)

