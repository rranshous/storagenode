import hashlib
import shove

# info about us (this node) and what we know
node_info = shove.Shove()

# lookup of the local file system's file's infos
local_file_lookup = node_info.setdefault('local_file_lookup', {})

# lookup of the entire networked file system's file's infos
all_file_lookup = node_info.setdefault('all_file_lookup', {})

# set of the events we've seen recently
recent_events = node_info.setdefault('recent_events', set())

# lookup of known nodes
nodes_lookup = node_info.setdefault('nodes_lookup', {})


def is_event(data):
    """
    returns bool, True if the passed data
    is a serialized event message
    """
    return data.startswith('!E')


def deserialize_event(data):
    """
    given the serialized event data
    will return (event, data)
    """
    pass


def serialize_event(event, data):
    """
    given the event and it's data will
    return string representation
    """
    pass


def send_event(name, **kwargs):
    """
    sends an event message to all connected nodes
    adds to the event's data info about this (sending) node
    """
    pass


def send_to_nodes(data):
    """
    sends raw data to all connected nodes
    """
    pass


def get_sha256(path):
    """
    returns the sha256 for the data at the given path
    """
    sha = hashlib.sha256()
    with open(path, 'r') as fh:
        while sha.update(fh.read(512)):
            pass
    return sha.hexdigest()


@contextmanager
def get_node_details(uuid):
    """
    makes available the dictionary for
    the node in our lookup
    """

    lookup = nodes_lookup.setdefault(uuid, {})
    yield lookup
    node_info.sync()


@contextmanager
def get_file_details(path, local=True):
    """
    returns a modifiable dictionary of the
    file's details
    """

    # start w/ our file lookup
    if local:
        lookup = local_file_lookup
    else:
        lookup = all_file_lookup

    # walk down by dir
    for piece in path.split(os.sep):
        lookup = lookup.setdefault(piece, {})
    yield lookup

    # sync the changes back to storage
    node_info.sync()


def update_file_db(path):
    """
    updates the file lookup w/ the newly updated
    file's details
    """

    # get the files details dict
    with get_file_details(path) as file_details:

        # see if the data has changed
        existing_sha = file_details.get('sha256')
        new_sha = get_sha(path)

        # did we change the data ?
        file_updated = existing_sha and existing_sha != new_sha

        # if we didn't update, nothing to do
        if not file_updated:
            return False

        # set the hash for the file
        file_details['sha256'] = get_sha(path)

        # set the modified time
        now = time.time()
        file_details['mtime'] = now

        # set the size
        file_details['size'] = os.path.getsize(path)

        # is this a new file ?
        new_file = not file_details.get('ctime')
        if new_file:

            # set it's created time
            file_details['ctime'] = now

            # set it's path
            file_details['path'] = path

        # put off our events about this file change
        if new_file:
            send_event('file_added', **file_details)
        else:
            send_event('file_updated', **file_details)



def read_file(path):
    """
    returns a generator which will output
    the file's data
    """
    pass


@contextmanager
def write_file(path):
    """
    context manager for writing data to new file
    """

    # give up the fh for them to write to
    with open(path, 'w') as fh:
        yield fh

    # insert / update our record in the DB
    update_file_db(path)


# handler for socket messages
def handle_socket_message(data):
    """
    called with data representing the message
    we've received from another node.
    Filter's recently seen messages.
    relay's all message's to connected nodes
    """

    # filter message's we've already seen
    if data in recently_seen:
        return False
    else:
        recently_seen.add(data)

    # pass to all connected nodes
    send_to_nodes(data)

    # check if it's an event
    if is_event(data):
        handle_event(*deserialize_event(data))

    else:
        raise Exception('Unknown message type: %s' % data)


# handler for events
def handle_event(event, data):
    """
    calls the passed events handler, passing along
    event and data.
    """

    # run our handler for the event
    handler = globals().get('handle_event_'+str(event))
    if not handler:
        print 'HANDLER not found for %s' % event
    else:
        try:
            handler(event, data)
        except Exception:
            print 'HANDLER ERROR: %s' % event
            raise


## handlers for all event types
def handle_event_file_added(event, data):
    """
    handle messages that another node has added
    a file. Add that file to our all lookup
    """

    # add is basically same as update
    handle_event_file_updated(event, data)

def handle_event_file_updated(event, data):
    """
    handles messages that a file has been updated
    on another node. update our details on that file
    """

    # update our global lookup
    path = data.get('path')
    with get_file_details(path, local=False) as file_details:
        file_details.extend(data)

def handle_event_file_deleted(event, data):
    """
    handles messages that a file was deleted on another
    node. updates our lookup, removing the file
    """

    # update our global lookup
    path = data.get('path')
    with get_file_details(path, local=False) as file_details:
        del file_details

def handle_event_node_found(event, data):
    """
    handles messages that a connected node
    has found another node. updates our node lookup
    """

    with get_node_details(node_uuid) as node_details:
        node_details.update(data)

def handle_event_node_lost(event, data):
    """
    handles messages from nodes that they
    lost contact with another node.
    """
    pass

def handle_event_file_search(event, data):
    """
    handles messages from nodes which are requesting
    details on file(s)
    """

    # see if we have the file in our

def handle_event_file_request(event, data):
    """
    handles messages from another node wanting
    a copy of a file
    """
    pass


# WSGI handler for when we are pushed file data
class PushHandler(object):

    def __init__(self, path):
        self.path = path

    def __call__(self, environ, start_response):
        """
        we should have file data streamed to us
        save it to the disk at our given path
        """

        # open our file to write to
        with write_file(self.path) as fh:
            # write down the data as it comes
            while data = environ.read(512):
                fh.write(data)

        # we're done
        start_response('200 OK', [('Content-Type','text-html')])
        return '1'
