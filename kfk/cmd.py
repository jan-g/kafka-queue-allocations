import collections
import hashlib
import heapq
import logging
from kafka import KafkaConsumer, KafkaProducer

from .parser import main as _main

LOG = logging.getLogger(__name__)



def main():
    _main([
        ("post =value", post),
        ("poll", poll),
        ("allocate +node", allocate),
        ("allocate --other", None, {"nargs": "*"}),
    ])


def post(args):
    LOG.debug("posting %s", args.value)

    client = KafkaProducer(batch_size=0)

    client.send("mytopic", bytes(args.value, "utf-8"))
    client.flush()


def poll(args):
    LOG.debug("polling")

    client = KafkaConsumer("mytopic", group_id="mygroup")
    try:
        result = client.poll(1000)
        if result is None:
            LOG.debug("polled none")
            return

        LOG.debug("result is %s", result)

        for topic, records in result.items():
            LOG.debug("topic is %s", topic)

            for record in records:
                LOG.debug("returned message: %s", record)
    finally:
        client.close()


def load_work(group, topic):
    """
    Return a dict of {TopicPartition: [items]}
    """
    client = KafkaConsumer(topic, group_id=group)
    try:
        result = client.poll(1000)
        if result is None:
            LOG.debug("polled none")
            return

        LOG.debug("result is %s", result)

        return result
    finally:
        client.close()


def path(cr):
    """
    Given a Kafka ConsumerRecord, extract the path information (which is the thing
    used to work out where the job should go), and return that
    :param cr: kafka queue item
    :return: the unique identifier of the corresponding function
    """
    return str(cr.value, "utf-8")


def sort_work(items):
    """
    Given {TopicPartition: [ConsumerRecord]}, return {path(cr): [ConsumerRecord]}
    :param items: 
    :return: dict
    """
    result = collections.defaultdict(list)

    for topic, records in items.items():
        LOG.debug("topic is %s", topic)

        for record in records:
            LOG.debug("returned message: %s", record)
            result[path(record)].append(record)

    return result


def score(path, node):
    """
    Given a path and a node, return a sortable score value for it.
    :param path: the path extracted from teh work item on the kafka queue
    :param node: the node name
    :return: a score - a sortable value
    """
    return hashlib.sha256(bytes(path, "utf-8") + bytes(node, "utf-8")).hexdigest()


def allocate(args, all_nodes=None, load_work=load_work):
    """
    Given a list of nodes, poll from the queue and attribute as many samples
    to their best-fit nodes.

    Report the results.

    Calculate the metric for the fit (which measures, how does this attribution
    compare versus how these would be placed on the same nodes, if each node
    had infinite capacity and the synchronous placement algorithm were in use).

    This is the basic approach used by the proposed golang version, without the
    details about keeping the buffer full, committing work, etc.
    """

    nodes = set(args.node)
    if all_nodes is None:
        all_nodes = set(args.other if args.other is not None else [])
    all_nodes.update(nodes)

    # Load the current crop of work
    unsorted_work = load_work("mygroup", "mytopic")
    work = sort_work(unsorted_work)

    # For each node, score it using the scoring function for every category
    # of available work.
    allocations = [(score(p, node), p, node)
                   for node in nodes
                   for p in work]
    heapq.heapify(allocations)

    assignments = {}

    # Used for calculating placement metrics
    ideals = {p: [node
                     for _, _, node in sorted(
                          [(score(p, node), p, node) for node in all_nodes])]
              for p in work}

    # Work out the best placement using the sync algorithm to the subset we have
    # If other nodes == {} then the total score for this would be 0; that might
    # be increased if the "ideal" node is not in the palcement set.
    best = {p: [node
                for node in ideals[p]
                if node in nodes][0]
            for p in ideals}

    # Let's also produce a random-ish fcfs score
    fcfs = {}
    fcfs_nodes = set(nodes)
    for partition in unsorted_work:
        if len(fcfs_nodes) == 0:
            break
        for cr in unsorted_work[partition]:
            if len(fcfs_nodes) == 0:
                break
            fcfs[fcfs_nodes.pop()] = cr

    # Pull stuff from the heap and allocate it whilst we still have nodes to allocate
    while len(nodes) > 0 and len(work) > 0:
        sc, p, node = heapq.heappop(allocations)
        LOG.debug("considering %s for %s and %s", sc, p, node)

        if node not in nodes:
            # Already allocated
            continue

        if p not in work:
            # Nothing left
            continue

        LOG.debug("  allocating %s to %s", p, node)
        items = work[p]
        assignments[node] = items.pop(0) # We should mark this as assigned
        nodes.remove(node)
        if len(items) == 0:
            del work[p]  # Nothing left here

    # We now have the full assignment of jobs to work.
    # Compare with the idealised synchronous placement.
    print("Assignments:")
    for node in assignments:
        p = path(assignments[node])
        print(node, p, ideals[p].index(node))
    print()

    print("Ideal ordering:")
    for p in ideals:
        print(p, ideals[p])
    print()

    print("Best assignment using sync allocation and unbounded capacity:")
    for p in best:
        print(best[p], p, ideals[p].index(best[p]))
    print()

    print("FCFS (random) assignments:")
    for node in fcfs:
        p = path(fcfs[node])
        print(node, p, ideals[p].index(node))
    print()

    print("Unallocated nodes:")
    print(nodes)
    print()

    print("Unallocated work:")
    for p in work:
        print(p, len(work[p]))
    print()
