# Kafka asynchronous job-scheduling: various algorithms, a metric for performance.

A rendezvous-style algorithm is used by Fn to give a 'sticky' allocation of function
id (more accurately, (app, route) pair) to node.

We'd like the scheduling of asynchronous work to line up as closely as possible with
where the synchronous scheuling would put functions - since those nodes are most likely
to have the right containers up and running in a hot fashion.

Take the "ideal" situation to be equivalent to the synchronous placement of functions
onto nodes with unbounded capacity. How do various queuing/scheduling approaches measure up?

# Simple metric

For each fnid of (app, route), we can hash and sort the potential nodes it *might* land on
and sort the result (this is what the fnlb does to place work). This gives us a mapping from
fnid to some *permutation* of [n0, n1, ...]. A simple measure is to look at where the
scheduling decision places a particular queued function in terms of position in that list.

Given a batch of function invocations to place, and a set of nodes that are requesting work,
we can simply look at what the resulting map of node->workitem is, and tot up the index
of that node in the ideal list of node placement for that workitem. (Other variations of
this kind of metric are definitely possible.)

A totally ideal situation will produce a result of 0, assuming that the preferred nodes
for the waiting workitems are amongst those requesting more asynchronous work to do.

FCFS will score somewhat more highly; we might consider the random allocation of work like this
to be the worst case.

What we want is some kind of tradeoff that'll keep the score for a batch of work placement
low, whilst still guaranteeing that queued work will be dealt with (and won't be overtaken
by new work *indefinitely*, although some reordering is permissable).

## Kafka considerations

When we poll for work from an API node, kafka delivers a batch of workitems (up to a configurable
maximum). For strict queues, that batch size == 1 (or 0). For kafka partitions, the batch size
may be larger.

We may place work on a node immediately, in which case we'll get a total FCFS behaviour; or we
might wait when a request comes in for some configurable period to let other nodes apply for work.

Even with a batch size of 1, waiting for other nodes for some period lets us place that work on a
"better" node according to the rendezvous algorithm.

If we combine a larger batch size with the wait-for-a-bit approach to dealing with node requests,
then we can potentially allocate workitems from that batch in a way that *approximately* minimises
the resulting total score. Continue until we're out of nodes or out of work. (Successive requests
from nodes will lead to higher metric values for those allocations, most likely, as the batch of
available workitems is denuded.)

(If a batch is exhausted, a reasonable approach is to return "no work" to nodes that we've not
found a fit for. Due to the way that kafka partitions are allocated to API nodes, that'd let the
node re-poll, that request potentially landing on another API node, which may well have work that
it can handle.)

The questions then are: is this good enough? Does the benefit over something like FCFS make it
worthwhile for the (moderately increased) work complexity?


## Running the simulation

The following invocation simulates continued delivery of batches of work to nodes.

There are 60 (app, route) functions; we simulate kafka having a batch of 15 of these jobs
in its queue every time it's polled. We simulate there being 10 nodes, of which 3 are asking
for work at the point an evaluation happens. We perform 200 runs.

    kexp --debug simulate 60 15 10 3 200

### Interpreting the output

We evaluate the metric above for the actual allocation of work performed, versus the best
we might possibly do given the nodes making requests (assuming they had unbounded capacity),
and also compare a random assignment of the work (simulating a simple FCFS strategy). The
final value roughly scales the actual score against the random score, to give an idea of how
well the actual score performs.

You'll occasionally see a random allocation *outperform* the PQ-based mechanism. That's
because the PQ is sorted by the rendezvous hash value rather than the metric score, so sometimes
we choose to do work that's less optimal because the hash values it has are more insistent.
These occasions show up as negative scaled scores.

A scaled score of 0 means "no better than FCFS". A scaled score of 100 means "as good as the ideal
placement". The occasional negative score means the hash artifact described above has occurred.
Because three work items are allocated per run from a selection of 15, we'd expect the scores
to fall slightly with a periodicity of five runs, as the sorting algorithm gets less and less
elbow-room in handling the current kafka batch.
