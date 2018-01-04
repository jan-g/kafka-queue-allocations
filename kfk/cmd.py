import collections
import hashlib
import heapq
import logging
import random
from kafka import KafkaConsumer, KafkaProducer

from .parser import main as _main

LOG = logging.getLogger(__name__)



def main():
    a = Allocator()

    _main([
        ("post =value", post),
        ("poll", poll),
        ("allocate +node", lambda args: a.allocate(args.node, args.other or [])),
        ("allocate --other", None, {"nargs": "*"}),
        ("simulate =num_funs =fun_batch =num_nodes =node_batch =runs", simulate)
    ])

    a.close()


def simulate(args):
    funs = ["f{}".format(i) for i in range(int(args.num_funs))]
    all_nodes = ["n{}".format(i) for i in range(int(args.num_nodes))]
    node_batch = int(args.node_batch)
    loader = SimulatedWorkLoader(funs, int(args.fun_batch))
    allocator = Allocator(loader=loader)
    results = []
    for i in range(int(args.runs)):
        print("run#", i)
        _, (actual, best, rand) = allocator.allocate(random.sample(all_nodes, node_batch), all_nodes)
        if rand == best:
            perc = 100
        else:
            perc = 100 * (1.0 - (actual - best) / (rand - best))
        results.append((actual, best, rand, perc))
        print()
        if perc > 100:
            break

    print("Result summary: run, actual score, best score, random score, %")
    for i, (a, b, r, p) in enumerate(results):
        print(i, a, b, r, p)


def post(args):
    LOG.debug("posting %s", args.value)

    client = KafkaProducer(batch_size=0)

    client.send("mytopic", bytes(args.value, "utf-8"))
    client.flush()


def poll(args):
    LOG.debug("polling")

    loader = WorkLoader("mygroup", "mytopic")
    try:
        result = loader.load_work()
        if result is None:
            LOG.debug("polled none")
            return

        LOG.debug("result is %s", result)

        for topic, records in result.items():
            LOG.debug("topic is %s", topic)

            for record in records:
                LOG.debug("returned message: %s", record)
    finally:
        loader.close()


class WorkLoader:
    def __init__(self, group, topic):
        self.client = KafkaConsumer(topic, group_id=group)

    def load_work(self):
        result = self.client.poll(1000)
        if result is None:
            LOG.debug("polled none")
            return

        LOG.debug("result is %s", result)

        return result

    def close(self):
        self.client.close()


class SimulatedWorkLoader:
    def __init__(self, all_paths, batch_size):
        self.all_paths = list(all_paths)
        self.batch_size = batch_size

    def close(self):
        pass

    def load_work(self):
        return {0: map(CR, random.choices(self.all_paths, k=self.batch_size))}


class CR:
    """Cheap and cheerful pretend ConsumerRecord"""
    def __init__(self, value):
        self.value = bytes(value, "utf-8")


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


class Allocator:
    def __init__(self, loader=None):
        if loader is None:
            loader = WorkLoader("mygroup", "mytopic")
        self.loader = loader
        self.work = None

    def close(self):
        self.loader.close()

    def get_batch(self):
        if self.work is None or len(self.work) == 0:
            unsorted_work = self.loader.load_work()
            self.work = sort_work(unsorted_work)
        return self.work

    def allocate(self, nodes, all_nodes=None):
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

        nodes = set(nodes)

        # all_nodes is just used for metric calculation to look at varying
        # performance approaches
        if all_nodes is None:
            all_nodes = set()
        all_nodes = set(all_nodes)
        all_nodes.update(nodes)

        # Load the current or next crop of work
        work = self.get_batch()

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
        # be increased if the "ideal" node is not in the placement set.
        best = {p: [node
                    for node in ideals[p]
                    if node in nodes][0]
                for p in ideals}
        counts = {p: len(work[p]) for p in work}

        # Let's also produce a random-ish fcfs score
        jobs = []
        for p in work:
            jobs.extend([p] * len(work[p]))
        random.shuffle(jobs)

        fcfs = {n: p
                for (n, p) in zip(nodes, jobs)}

        print("Work as follows")
        print("Work items:")
        for p in work:
            print(p, len(work[p]))
        print()
        print("Nodes:", nodes)
        print()

        # The real allocation algorithm follows

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
        score_actual = 0
        for node in assignments:
            p = path(assignments[node])
            sc = ideals[p].index(node)
            score_actual += sc
            print(node, p, sc)
        print()

        print("Ideal ordering:")
        for p in ideals:
            print(p, ideals[p])
        print()

        print("Best assignment using sync allocation and unbounded capacity:")
        score_best = []
        for p in best:
            sc = ideals[p].index(best[p])
            score_best.extend([sc] * counts[p])
            print(best[p], p, sc)
        print()
        score_best = sum(sorted(score_best)[:len(assignments)])

        print("FCFS (random) assignments:")
        score_random = 0
        for node in fcfs:
            p = fcfs[node]
            sc = ideals[p].index(node)
            score_random += sc
            print(node, p, ideals[p].index(node))
        print()

        print("Unallocated nodes:")
        print(nodes)
        print()

        print("Unallocated work:")
        for p in work:
            print(p, len(work[p]))
        print()

        return assignments, (score_actual, score_best, score_random)
