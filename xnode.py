import threading
import atexit
import sys, time
import collections
import weakref
from copy import deepcopy
import socket
from datetime import datetime

#from FixedList import FixedList

try:
    import os, gevent
    if os.fork == gevent.fork:
        import zmq.green as zmq
    else:
        import zmq
except:
    import zmq


def pr_red(prt): print("\033[91m {}\033[00m".format(prt))


def pr_green(prt): print("\033[92m {}\033[00m".format(prt))


def concat_paths(path_start, path_end):
    if path_start.endswith('/'):
        path_start = path_start[:-1]
    if path_end.startswith('/'):
        return path_start + path_end
    return path_start + '/' + path_end


class XObj(object):
    pass


TIMESTAMP_SLOT = 0


def do_nothing():
    pass

enabled_features = dict()


class XPathNode:

    def __init__(self, father_pnode, name, obj=None, allow_ref=False, toggle_feature=None):
        self.father = father_pnode
        self.name = name
        self.sons = dict()
        self.xobj = None
        self.logs = None
        self.scheme = collections.OrderedDict()
        self.toggle_feature = toggle_feature
        if obj is not None:
            self.set_xobj(obj, allow_ref)

    def set_xobj(self, obj, allow_ref):
        try:
            self.xobj = weakref.ref(obj, lambda ref, rem_obj=obj: xremove(rem_obj))
        except TypeError:
            self.xobj = obj
            if not allow_ref:
                raise TypeError("use xremove(obj=your_obj) when done, "
                                "otherwise gc will not collect this object")

    def get_xobj(self):
        if type(self.xobj) == weakref.ref:
            return self.xobj()
        return self.xobj


root_xpath_node = XPathNode(None, '/', allow_ref=True)
obj_to_path = dict()


def add_default_slots(scheme):
    scheme['.timestamp'] = TIMESTAMP_SLOT


def add_scheme(pnode, obj, is_log):
    if len(pnode.scheme) == 0 and is_log:
        add_default_slots(pnode.scheme)
    scheme = decompose_fields(obj, show_scheme=True, show_data=False)
    if scheme:
        for slot in scheme[0]:
            if slot not in pnode.scheme:
                pnode.scheme[slot] = len(pnode.scheme)


def put_in_right_slots(scheme, curr_scheme, data):
    new_data = [""]*len(scheme)
    for idx, slot in enumerate(curr_scheme):
        new_data[scheme[slot]] = data[idx]
    return new_data


def timestamp():
    return int(round(time.time() * 10000))


def xadd(obj, full_path, allow_ref=False, log=False, toggle_feature=None):
    """
        allow_ref -- allow ref. for built-in types weak references are not allowed,
                   if use_ref is False, an exception will be raised
    """
    paths = full_path.split('/')
    name = paths.pop()
    subpaths = paths
    if id(obj) in obj_to_path and not log:
        xremove(obj_to_path[id(obj)])
    if callable(obj):
        obj = obj()
    father_pnode = root_xpath_node
    for sxpath in subpaths:
        if sxpath == '':
            continue
        if sxpath not in father_pnode.sons:
            xpath = XPathNode(father_pnode, sxpath, allow_ref=allow_ref, toggle_feature=toggle_feature)
            father_pnode.sons[sxpath] = xpath
            father_pnode = xpath
        else:
            father_pnode = father_pnode.sons[sxpath]
    save_obj = None if log else obj
    pnode = father_pnode.sons[name] if name in father_pnode.sons \
        else XPathNode(father_pnode, name, save_obj, allow_ref=allow_ref, toggle_feature=toggle_feature)
    father_pnode.sons[name] = pnode
    #if not pnode.scheme: # TODO: to support dynamic scheme, how to add static scheme for performance???
    add_scheme(pnode, obj, log)

    if log:
        obj_copy = deepcopy(obj)
        dec_obj = decompose_fields(obj_copy, show_scheme=True, show_data=True)
        if not dec_obj:
            return
        if pnode.logs is None:
            pnode.logs = FixedList(128)
        data = put_in_right_slots(pnode.scheme, dec_obj[0], dec_obj[1])
        data[TIMESTAMP_SLOT] = timestamp()
        if dec_obj:
            #TODO: if empty data ?
            pnode.logs.append(data)
        return
    obj_to_path[id(obj)] = full_path


# save scheme on save in xadd
def xlog(path, obj):
    xadd(obj, path, log=True)


class PathNotExistsError(Exception):
    pass


def xremove(path=None, obj=None):
    if obj:
        try:
            path = obj_to_path[id(obj)]
        except KeyError:
            return
    subpaths = path.split('/')
    father_xpath_node = root_xpath_node
    nodes_path = list()
    nodes_path.append(('/', root_xpath_node))
    for subpath in subpaths:
        if subpath == '':
            continue
        if subpath in father_xpath_node.sons:
            nodes_path.append((subpath, father_xpath_node.sons[subpath]))
            father_xpath_node = father_xpath_node.sons[subpath]
        else:
            raise PathNotExistsError()
    son_subpath, son_node = nodes_path.pop()
    nodes_path.reverse()
    for subpath, node_path in nodes_path:
        if len(son_node.sons) == 0:
            xobj = son_node.get_xobj()
            if xobj and id(xobj) in obj_to_path:
                del obj_to_path[id(xobj)]
            del node_path.sons[son_subpath]
            son_node = node_path
            son_subpath = subpath
        else:
            break


def _all_paths(father_path, full_path, ret):
    if len(father_path.sons) == 0:
        ret[full_path] = father_path
    for (path_name, xpath) in father_path.sons.items():
        _all_paths(xpath, full_path + path_name + '/', ret)


def all_paths():
    ret = dict()
    _all_paths(root_xpath_node, "/", ret)
    return ret


def toggle_feature(pnode):
    if pnode in enabled_features:
        enabled_features[pnode] = datetime.utcnow()
    else:
        if pnode.toggle_feature:
            pnode.toggle_feature()
            enabled_features[pnode] = datetime.utcnow()


def list_path(path):
    father_pnode = root_xpath_node
    dirs = path.split('/')[1:]
    try:
        if dirs[-1] == '':
            dirs = dirs[:-1]
    except IndexError:
        pass
    for dir in dirs:
        if dir in father_pnode.sons:
            father_pnode = father_pnode.sons[dir]
        else:
            return None, None, ""
    xobj = father_pnode.get_xobj()
    if not father_pnode.scheme:
        add_scheme(father_pnode, xobj, False)
    if xobj is not None:
        toggle_feature(father_pnode)
        return xobj, father_pnode.scheme, "XOBJ"
    if father_pnode.logs is not None:
        return father_pnode.logs, father_pnode.scheme, "LOGS"
    return father_pnode.sons, None, "DIR"


def is_string(field):
    if sys.version_info >= (3, 0, 0):
        # for Python 3
        if isinstance(field, str):
            return True
    else:
        # for Python 2
        if isinstance(field, str) or isinstance(field, unicode):
            return True
    return False


def is_container_field(field):
    if isinstance(field, collections.Iterable) and not is_string(field) and not isinstance(field, tuple):
        return True
    return False


def order_scheme(obj, scheme):
    if getattr(obj, '__xslots_order__', None):
        ordered_dict = collections.OrderedDict()
        for slot in obj.__xslots_order__:
            ordered_dict[slot] = True
        for slot in scheme:
            if slot not in ordered_dict:
                ordered_dict[slot] = True
        scheme = list(ordered_dict.keys())
    return scheme


def inst_fields(xobj):
    scheme = [attr for attr in dir(xobj) if not attr.startswith('__')
              and not attr.startswith('_')
              and not callable(getattr(xobj, attr))]
    return order_scheme(xobj, scheme)


def is_simple_field(field):
    if sys.version_info >= (3, 0, 0):
        # for Python 3
        return type(field) in [int, bool, str, float]
    else:
        # for Python 2
        return type(field) in [int, bool, str, float, long, unicode]
    return False


def decompose_simple(xobj):
    ret_fields = collections.OrderedDict()
    for attr_name in inst_fields(xobj):
        attr = getattr(xobj, attr_name)
        if is_simple_field(attr):
            ret_fields[attr_name] = attr
    return list(ret_fields.values()), list(ret_fields.keys())


def append_data(ret_items, item, show_scheme, show_data):
    data, scheme = decompose_simple(item)
    if show_scheme:
        ret_items.append(scheme)
    if show_data:
        ret_items.append(data)
    return ret_items


def add_cross_if_dir(name, son_dir):
    if son_dir.sons and len(son_dir.sons) > 0:
        return name + '/'
    return name


def decompose_fields(obj_inspect, show_scheme, show_data, ret_fields=None, it=0):
    if ret_fields is None:
        ret_fields = list()
    if is_container_field(obj_inspect):
        inspect = obj_inspect
        if isinstance(obj_inspect, dict):
            inspect = list(obj_inspect.values())
            if inspect and is_simple_field(inspect[0]):
                extrtact_simple_dict(inspect, obj_inspect, ret_fields, show_data, show_scheme)
                return ret_fields
        if show_scheme:
            inspect = inspect[:1]
        for field in inspect:
            decompose_fields(field, show_scheme, show_data, ret_fields, it)
        return ret_fields
    else:
        return append_data(ret_fields, obj_inspect, show_scheme, show_data)


def extrtact_simple_dict(inspect, obj_inspect, ret_fields, show_data, show_scheme):
    if show_scheme:
        ret_fields.append(list(obj_inspect.keys()))
    if show_data:
        ret_fields.append(inspect)


def xdump(path, show_scheme=True, show_data=True):
    # print "query_res " + str(xquery_res)
    xobj, scheme, ret_type = list_path(path)
    if xobj is None:
        return None
    if ret_type == "DIR":
        ret_fields = [['dir']]
        for (son_dir_name, son_dir) in xobj.items():
            ret_fields.append([add_cross_if_dir(son_dir_name, son_dir)])
        return ret_fields
    ret_fields = list()
    if show_scheme:
        ret_fields.append(list(scheme.keys()))
    if ret_type == "LOGS":
        ret_fields.extend(xobj)
        return ret_fields
    def_interval = sys.getcheckinterval()
    sys.setcheckinterval(1000000000)  # TODO: maybe copy before and no need to lock?
    try:
        ret_fields.extend(decompose_fields(xobj, show_scheme=False, show_data=show_data))
    except Exception as e:
        raise e
    finally:
        sys.setcheckinterval(def_interval)
    return ret_fields


def xnode_id(node_type, node_index, key, host_name):
    return '{}:{}:{}:{}'.format(node_type, node_index, key, host_name)


def str_to_xnode_id(xnode_id_str):
    node_type, node_index, cluster_key, hostname = xnode_id_str.split(':')
    return node_type + '-' + node_index, cluster_key, hostname


class Counters(object):
    num_handled_q = 0
    total_time = 0

    err_rcv_timeout = 0


def expire_features():
    delete_features = []
    for (feature, ts) in enabled_features.items():
        if (datetime.utcnow() - ts).total_seconds() > 10:
            delete_features.append(feature)
            feature.toggle_feature()
    for feature in delete_features:
        del enabled_features[feature]


def expire_all_features():
    for (feature, ts) in enabled_features.items():
        feature.toggle_feature()
    enabled_features.clear()


class XNodeClient(object):
    XRAY_CFG_ENV = "XRAY_CONFIG"
    XRAYIO_HOST = "www.xrayio.com"
    XRAYIO_PORT = "40001"

    ctx = zmq.Context()
    req = ctx.socket(zmq.DEALER)
    nid = None

    def send_msg_to_server(self, rs, req_id="", ts=0, avg_ms=0, widget_id=""):
        response = dict()
        response["req_id"] = req_id
        response["result_set"] = rs
        response["timestamp"] = ts
        response["avg_load_ms"] = avg_ms
        response["widget_id"] = widget_id
        print("Sending: %s" % response)
        self.req.send(''.encode('ascii'), zmq.SNDMORE)
        self.req.send_json(response)

    def rec_msg_from_server(self):
        json = self.req.recv_json()
        print("Recved: %s" % json)
        return json["req_id"], json["query"], json["timestamp"], json["widget_id"]

    def xnode_run(self, node_type, node_index, cluster_key, host_name):
        print("staring xnode_run")
        tcpStr = 'tcp://{xrayio_host}:{port}'.format(xrayio_host=self.XRAYIO_HOST, port=self.XRAYIO_PORT)
        self.nid = xnode_id(node_type, node_index, cluster_key, host_name)
        while True:
            self.req.setsockopt(zmq.IDENTITY, self.nid.encode('ascii'))
            self.req.setsockopt(zmq.RCVTIMEO, 30000)
            self.req.connect(tcpStr)
            self.send_msg_to_server([['xnode-ok-{}'.format(self.nid)]])
            while True:
                try:
                    # expire_features()
                    req_id, query, ts, widget_id = self.rec_msg_from_server()
                    before_run_ts = datetime.utcnow()
                    if query == '/..err-no_cluster':
                        print("cluster '%s' not exists, exiting" % cluster_key)
                        return
                    if query == '/..quit':
                        return
                    if query == '/..ping':
                        rs = [['pong']]
                        self.send_msg_to_server(rs)
                        continue
                    rs = xdump(query)
                    avg_ms = int((datetime.utcnow() - before_run_ts).total_seconds() * 1000)
                    self.send_msg_to_server(rs, req_id, ts, avg_ms, widget_id)
                except zmq.ZMQError:
                    pr_red("CONNECTION TO SERVER LOST...RECONNECTING")
                    # expire_all_features()
                    self.req.close()
                    self.req = self.ctx.socket(zmq.DEALER)
                    break

    def send_last_results(self):
        time.sleep(5)  # for low letancy connections
        print("SENDING EXIT")
        self.send_msg_to_server(['xnode-exit-{}'.format(self.nid),
                                 list(all_paths().keys())], '-1', 0)

xclient = XNodeClient()


def xnode_args():
    import argparse
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--key',
                        help='key',
                        required='True')
    results = parser.parse_args(sys.argv[1:])
    return results


def xnode_client(node_type, cluster_key=None, node_index='', host_name=None):
    if host_name is None:
        host_name = socket.gethostname()
    atexit.register(xclient.send_last_results)

    if not cluster_key:
        result = xnode_args()
        cluster_key = result.key

    thr = threading.Thread(target=xclient.xnode_run, args=(node_type, node_index, cluster_key, host_name))
    thr.daemon = True
    thr.start()

