using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server.Utils;
using Voron.Data.Containers;
using Voron.Util;
using Array = System.Array;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    /*
     * The problem with HNSW is that it is a graph algorithm, which requires
     * that we'll touch significantly more nodes than we would usually do in a B+Tree, 
     * for example. 
     * 
     * If we need to index 1M items, using a B+Tree, I can sort them and be sure that I 
     * can get pretty good disk access patterns. For HNSW - the problem is that we need to
     * do effectively random I/O for each lookup. Sequential HNSW is running this one node 
     * at a time, which link each node to its nearest neighbors. It has horrible performance
     * once you exceed the size of memory on the machine.
     * 
     * Adding 1M nodes to a HNSW graph with 15M nodes is _expensive_. Assume that they use 768 dimensions
     * and no quantization. That 15M * 768 * 4 = 42GB of data just for the vectors. And adding a new node
     * means that we need to compare (and thus read, randomly) about 600 vectors (with peaks of 2,000 vectors). 
     * 
     * Typically, the solution for that is to get a bigger machine, but that is something that we can
     * try to address. This is the purpose of the code in this file. We go through many gymnastics to
     * try to optimize the disk access pattern and parallelize what we can.
     * 
     * Parallelization is complicated by the fact that we are running under a write transaction scope.
     * A write transaction in Voron is a _single threaded operation_. Another problem is that HNSW is
     * inherently a single-threaded algorithm. If I add two nodes to the graph, the second node will 
     * consider the first node as a candidate for its neighbors.
     * 
     * Moving to parallel mode make things more complex. If I add two nodes to the graph at the same time,
     * they will _not_ consider each other for neighbors. Given that HNSW is *approximate* nearest neighbor
     * algorithm, this is not too big an issue. We can assume that they will reside "nearby" and that the
     * greedy nature of the algorithm will find the right nodes.
     * 
     * But it does show that paralleling HNSW *will* impact the resulting graph. To address that, we added
     * a step in the process where we'll add all the existing inflight nodes (being added in parallel) to each
     * other. This means that we create artificial edges to vectors added at the same time. Those edges may be
     * removed if during the insert process we'll find better (closer) edges. 
     * 
     * Having said all of that, the performance difference for large graph is significant. Therefor, we 
     * use a parallel algorithm to build the graph. However, just adding threads isn't simple, because we
     * operate under a single threaded write transaction.
     * 
     * There are two expensive parts in the graph building operations:
     * * Computing distance between vectors
     * * Loading the vectors from disk
     * 
     * This code is designed to allow to parallelize the distance computation and to allow for
     * batch load optimization for reading the vectors. 
     * 
     * To start with, we aren't actually using parallel here to say threads. Instead, we re-wrote
     * the algorithm using yield an enumerators. And we run it using a dedicated runner that consumes and
     * execute all the interleaved (vs. concurrent) operations.
     * 
     * Whenever we need to do an expensive operation (such as loading vectors, or computing distances),
     * we yield to the caller, giving a chance for the rest of the system to make forward progress while
     * the task is completed in the background. 
     * 
     * That async operation is _not_ scheduled on a different thread. Instead, it is queued until all
     * current operations are completed, then we check what pending work we have and start a batch 
     * loads of all the vectors we need. The next step is to run the distance computation using the 
     * thread pool. When that is completed, we can continue with the next step.
     * 
     * The idea is that we run N interleaved tasks, where N between 1..MaxConcurrentBatches, and in 
     * each one of them, we pick an item to be inserted to the graph. We then run the HNSW until we
     * need to do an expensive operation (which we'll offload to the thread pool if it is computation, or
     * do a batch preload to amortise the costs of going to disk). At that point, we yield to *another*
     * interleaved operation. By the time we hit the MaxConcurrentBatches, we gathered enough vectors to load and
     * distances to compute that we can really start pumping through all the items. 
     * 
     * The key here is to batching of I/O for loading the vectors. See the runner for handling that 
     * part of the process. Both NodePlacement and NodePlacementRunner are working very closely 
     * together to achieve this work.
     * 
     * # Distance computation using the thread pool
     * 
     * Distance computation is expensive, and we want to run it in parallel. Each work item that we 
     * send to the thread pool already had its vectors loaded by the batch process, so we can assume 
     * that they are ready in memory. The work item compares a vector to a set of vectors (typically all 
     * the edges of a particular node) and returns the shortest distance or the filtered set of edges.
     * 
     * We use the thread pool because:
     * * There is a known limit to the amount of work we have (up to MaxConcurrentBatches), and it
     *   cannot grow without bound. We won't cause thread pool starvation.
     * * The amount of work for each item is well scoped and _short_. Under 0.5ms for each work item, 
     *   so we won't cause a bottleneck in the thread pool.
     * * We tested using a dedicated thread pool, but those performed significantly worse than the 
     *   default .NET one. 
     *
     * # Allocation-free work item dispatch
     *
     * The WorkItem base class implements IThreadPoolWorkItem, which allows us to queue it directly to
     * the .NET thread pool via ThreadPool.UnsafeQueueUserWorkItem without allocating a delegate or a
     * Task. Each concrete worker (ProcessEdgesWorker, FilterEdgesHeuristicWorker, FindNearestWorker)
     * is preallocated once per NodePlacement instance and stored in a field. The enumerators in
     * Process(), FindGraphPlacementForNode(), NearestEdges(), etc. yield the *same* preallocated
     * worker object repeatedly — mutating its state (CurrentNodeIndex, Level, Owner, etc.) before each
     * yield return. This means no new work items are heap-allocated during the graph-building loop;
     * the runner simply resets and re-queues the same objects. The trade-off is that a yielded WorkItem
     * is only valid until the enumerator advances, but that is fine because the runner always consumes
     * the item before calling MoveNext again.
     */
    public partial class Registration
    {
        private int _nextNodeIndex;

        public int MaxConcurrentBatches = 512;

        private void InsertVectorsToGraph(ref ContextBoundNativeList<byte> byteBuffer, CancellationToken token)
        {
            if (_searchState.TryGetLocationForNode(EntryPointId, out var entryPointNode) is false)
            {
                if (_searchState.CreatedNodes is 0)
                    return;
                
                _nextNodeIndex++; // do not attempt to insert the first node, since it is the graph root
                ref Node startingNode = ref _searchState.Nodes[0];
                Span<byte> span = startingNode.Encode(ref byteBuffer);
                var allocatedId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, span.Length, out Span<byte> allocated);
                entryPointNode = (long)allocatedId;
                span.CopyTo(allocated);
                _searchState.RegisterNodeLocation(EntryPointId, entryPointNode);
            }

            // Run 1..MaxConcurrentBatches batches here, depending on how much work we have to run
            int numberOfBatches = Math.Max(1, _searchState.CreatedNodes / MaxConcurrentBatches);
            // but not too much...
            int maxTasks = Math.Min(numberOfBatches, MaxConcurrentBatches);
            NodePlacementRunner runner = new(this, maxTasks, token);
            runner.Run();
        }

        private class NodePlacement(Registration parent, NodePlacementRunner runner)
        {
            private readonly SearchState _searchState = parent._searchState;
            private readonly List<int> _candidates = [];
            private readonly List<int> _nearestIndexes = [];
            private readonly List<int> _indexes = [];
            private readonly List<int> _requiresEdgeFiltering = [];
            private readonly List<UnmanagedSpan> _vectors = [];

            // Per-task, point-in-time copy of the current (node, level) edge indexes. Filled on the
            // LLT thread in PrepareEdgesOnLLT and read by the worker in PopulateWorkListsOnWorker.
            // This decouples the worker from the shared, growable EdgesIndexesPerLevel native buffer,
            // whose backing store the LLT thread can free/realloc in a later dispatch round while the
            // worker still runs (the PopulateWorkListsOnWorker use-after-free, RavenDB-26809). One
            // WorkItem per task is in flight at a time (WorkItem.Execute re-enqueues the iterator only
            // after the worker finishes), so this buffer is safe to clear+refill on every dispatch.
            private readonly List<int> _edgeIndexSnapshot = [];
            private readonly PriorityQueue<int, float> _candidatesQ = new();
            private readonly PriorityQueue<int, float> _nearestEdgesQ = new();
            private ulong[] _visitedBitmap = [];
            private int[] _visitedBitmapVersion = [];
            private int _visitedVersion;
            private readonly LinkedListNode<int> _listNode = new(-1);

            // Pooled work items — reused across all yields to avoid per-yield heap allocations
            private readonly ProcessEdgesWorker _processEdgesWorker = new(runner);
            private readonly FilterEdgesHeuristicWorker _filterEdgesWorker = new(runner);
            private readonly FindNearestWorker _findNearestWorker = new(runner);
            
            private void ClearVisited()
            {
                // this needs to be _cheap_, since it is called per node per level
                _visitedVersion++;
            }
            private bool MarkVisited(int pos)
            {
                int index = pos >> 6; // / 64
                int bit = pos & 63; // % 64
                
                if (index >= _visitedBitmap.Length)
                {
                    Grow();
                }

                if (_visitedBitmapVersion[index] != _visitedVersion)
                {
                    // we reset the value if detected the version changed
                    _visitedBitmapVersion[index] = _visitedVersion;
                    _visitedBitmap[index] = 0;
                }
                
                ulong old = _visitedBitmap[index];
                ulong mask = (1ul << bit);
                _visitedBitmap[index] = old | mask;
                bool isNew = (mask & old) == 0;
                return isNew;

                void Grow()
                {
                    int max = Math.Max(_searchState.Nodes.Length, index);
                    int newSize = Bits.NextAllocationSize(max);
                    Array.Resize(ref _visitedBitmap, newSize);
                    Array.Resize(ref _visitedBitmapVersion, newSize);
                }
            }

            public IEnumerable<WorkItem> Process()
            {
                _processEdgesWorker.Owner = this;
                _filterEdgesWorker.Owner = this;
                _findNearestWorker.Owner = this;

                try
                {
                    int createdNodesLength = _searchState.CreatedNodes;
                    while (runner.IsCancelled is false)
                    {
                        // shared across all tasks, we are processing 
                        // multiple nodes in an interleaved (but not concurrently) via
                        // multiple running placement processing at the same time
                        var createdNodeIndex  = parent._nextNodeIndex++;  
                        if (createdNodeIndex  >= createdNodesLength)
                            break;

                        // we do not process these in linear order, so we need
                        // to keep whatever is "in-flight" in a linked list that we can 
                        // cheaply add & remove to
                        var currentNodeIndex = _searchState.GetCreatedNodeIndex(createdNodeIndex); 
                        _listNode.Value = currentNodeIndex;
                        runner.AddInFlight(_listNode);
                        foreach (var item in FindGraphPlacementForNode(createdNodeIndex, currentNodeIndex))
                        {
                            yield return item;
                        }
                        runner.RemoveInFlight(_listNode);
                    }
                }
                finally
                {
                    runner.Done();
                }
            }

            private IEnumerable<WorkItem> FindGraphPlacementForNode(int createdNodeIndex, int currentNodeIndex)
            {
                var currentMaxLevel = _searchState.Options.CurrentMaxLevel(_searchState.CreatedNodes - createdNodeIndex);
                int nodeRandomLevel = GetLevelForNewNode(currentMaxLevel);
                UnmanagedSpan insertedVector;
                {
                    //  scoping n here, to avoid "leaking" the reference and async issues
                    ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    n.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, nodeRandomLevel + 1);
                    insertedVector = n.GetVectorUnmanagedSpan(_searchState);
                    AddEdgesFromInFlightNodes(ref n, createdNodeIndex);
                }

                // Inlined descent from the entry point down to level 0, capturing the closest
                // node at each level. Folding this into the placement loop removes the per-node
                // enumerator allocation that a yielding helper would produce.
                {
                    _nearestIndexes.Clear();
                    ClearVisited();
                    MarkVisited(currentNodeIndex);
                    var snalCurrentNodeIndex = _searchState.GetNodeIndexById(EntryPointId);
                    var snalLevel = currentMaxLevel;
                    var snalDistance = float.MaxValue;
                    while (snalLevel >= 0)
                    {
                        do
                        {
                            _findNearestWorker.Reset(insertedVector, snalCurrentNodeIndex, snalLevel);
                            yield return _findNearestWorker;
                            if (_findNearestWorker.Distance >= snalDistance)
                                break;
                            snalCurrentNodeIndex = _findNearestWorker.CurrentNodeIndex;
                            snalDistance = _findNearestWorker.Distance;
                        } while (true);

                        _nearestIndexes.Add(snalCurrentNodeIndex);
                        snalLevel--;
                    }

                    _nearestIndexes.Reverse();
                }
                
                for (int level = nodeRandomLevel; level >= 0; level--)
                {
                    int startingPointIndex = _nearestIndexes[level];

                    // Inlined NearestEdges(startingPointIndex, currentNodeIndex, insertedVector, level):
                    // beam-search candidate expansion + (conditional) heuristic edge filter. Inlined for
                    // the same reason as SearchNearestAcrossLevels — this is the deepest yield site,
                    // called once per level per node.
                    {
                        Debug.Assert(_candidatesQ.Count == 0);
                        Debug.Assert(_nearestEdgesQ.Count == 0);
                        Debug.Assert(startingPointIndex != currentNodeIndex);

                        float lowerBound = float.MaxValue;
                        ClearVisited();
                        MarkVisited(currentNodeIndex); // we can't have an edge to itself

                        _candidatesQ.Enqueue(startingPointIndex, -lowerBound);

                        while (_candidatesQ.TryDequeue(out var cur, out var curDistance))
                        {
                            if (-curDistance < lowerBound &&
                                _nearestEdgesQ.Count == _searchState.Options.NumberOfCandidates)
                                break;

                            _processEdgesWorker.Reset(insertedVector, lowerBound, cur, level);
                            yield return _processEdgesWorker;
                            lowerBound = _processEdgesWorker.LowerBound;
                        }

                        _candidatesQ.Clear();
                        _candidates.Clear();
                        while (_nearestEdgesQ.TryDequeue(out var edgeId, out _))
                        {
                            _candidates.Add(edgeId);
                        }
                        _candidates.Reverse();

                        if (_candidates.Count > _searchState.Options.NumberOfEdges)
                        {
                            _indexes.Clear();
                            _vectors.Clear();
                            foreach (var candidate in _candidates)
                            {
                                ref var cn = ref _searchState.GetNodeByIndex(candidate);
                                _indexes.Add(candidate);
                                _vectors.Add(cn.GetVectorUnmanagedSpan(_searchState));
                            }

                            // disable preloading - we already got everything from the
                            // previous preloading step and are operating purely in memory
                            _filterEdgesWorker.Reset(insertedVector, -1, level);
                            yield return _filterEdgesWorker;
                        }
                    }

                    PortableExceptions.ThrowIf<InvalidOperationException>(_candidates.Count == 0, "Cannot add a node to the graph without any edges");
                    ref var node = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    ref var list = ref node.EdgesPerLevel[level];
                    // important - we cannot reset here, since we have added edges from the in flight nodes in AddEdgesFromInFlightNodes()
                    list.EnsureCapacityFor(_searchState.Llt.Allocator, _candidates.Count);
                    _requiresEdgeFiltering.Clear();
                    foreach (var edgeIdx in _candidates)
                    {
                        Debug.Assert(edgeIdx != currentNodeIndex);
                        ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                        list.AddUnsafe(edge.NodeId);

                        ref var edgeList = ref edge.EdgesPerLevel[level];
                        edgeList.Add(_searchState.Llt.Allocator, node.NodeId);

                        // Mirror the append into EdgesIndexesPerLevel so RegisterForPreloading
                        // can skip its O(M) NodeId -> index rebuild. We only update when the
                        // cache is already populated for this level and was in sync before this
                        // append; otherwise we leave the lazy rebuild to fix it.
                        if (edge.EdgesIndexesPerLevel.Count > level)
                        {
                            ref var edgeIndexes = ref edge.EdgesIndexesPerLevel[level];
                            if (edgeIndexes.Count == edgeList.Count - 1)
                                edgeIndexes.Add(_searchState.Llt.Allocator, currentNodeIndex);
                        }

                        if (edgeList.Count <= _searchState.Options.NumberOfEdges)
                            continue;

                        _requiresEdgeFiltering.Add(edgeIdx);
                    }

                    foreach (var edgeIdx in _requiresEdgeFiltering)
                    {
                        UnmanagedSpan vector;
                        {
                            ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                            vector = edge.GetVectorUnmanagedSpan(_searchState);
                            ClearVisited();
                            MarkVisited(edgeIdx);
                        }

                        _filterEdgesWorker.Reset(vector, edgeIdx, level);
                        yield return _filterEdgesWorker;
                        
                        PortableExceptions.ThrowIf<InvalidOperationException>(_candidates.Count == 0 , "Cannot add a node to the graph without any edges after heuristic");
                        {
                            ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                            ref var edgeList = ref edge.EdgesPerLevel[level];
                            edgeList.ResetAndEnsureCapacity(_searchState.Llt.Allocator, _candidates.Count);
                            foreach (var idx in _candidates)
                            {
                                edgeList.AddUnsafe(_searchState.GetNodeByIndex(idx).NodeId);
                            }

                            // _candidates already holds node indexes, so we can rewrite the
                            // mirrored cache directly without touching the node id table.
                            if (edge.EdgesIndexesPerLevel.Count > level)
                            {
                                ref var edgeIndexes = ref edge.EdgesIndexesPerLevel[level];
                                edgeIndexes.ResetAndEnsureCapacity(_searchState.Llt.Allocator, _candidates.Count);
                                foreach (var idx in _candidates)
                                {
                                    edgeIndexes.AddUnsafe(idx);
                                }
                            }
                        }
                    }
                }
            }

            private void AddEdgesFromInFlightNodes(ref Node n, int createdNodeIndex)
            {
                // Here we add "number of edges" previously added items to as the edges in all their levels
                // so the next stage will add the edges that were already added to the graph and then find 
                // only the most suitable ones. It has the impact of increasing the likelihood that 
                // items that are added at the same time (and thus temporally linked, at least) will
                // be joined. Quite important when you consider that a single document may have multiple
                // vectors associated with it (for example, because of chunking).
                CollectionsMarshal.SetCount(_indexes, _searchState.Options.NumberOfEdges);
                var used = runner.GetInFlightIndexes(_listNode, CollectionsMarshal.AsSpan(_indexes));
                for (int i = 0; i < used; i++)
                {
                    ref var edge = ref _searchState.GetNodeByIndex(_indexes[i]);
                    int sharedLevels = Math.Min(edge.EdgesPerLevel.Count, n.EdgesPerLevel.Count);
                    for (int level = 0; level < sharedLevels; level++)
                    {
                        n.EdgesPerLevel[level].Add(_searchState.Llt.Allocator, edge.NodeId);
                    }
                }
                _indexes.Clear();
            }

            private sealed class FilterEdgesHeuristicWorker(NodePlacementRunner runner) : WorkItem(runner)
            {
                private UnmanagedSpan _src;

                public void Reset(UnmanagedSpan src, int currentNodeIndex, int level)
                {
                    _src = src;
                    CurrentNodeIndex = currentNodeIndex;
                    Level = level;
                }

                protected override void DoWork()
                {
                    // See: https://icode.best/i/45208840268843 - Chinese, but auto-translate works, and a good explanation with 
                    // conjunction of: https://img-bc.icode.best/20210425010212938.png
                    // See also the paper here: https://arxiv.org/pdf/1603.09320
                    // This implements the Fig. 2 / Algorithm 4

                    var searchState = Owner._searchState;
                    var candidates = Owner._candidates;
                    var vectors = Owner._vectors;
                    var indexes = Owner._indexes;
                    var queue = Owner._candidatesQ;
                    
                    Debug.Assert(queue.Count is 0);
                    for (int i = 0; i < indexes.Count; i++)
                    {
                        var distance = searchState.Distance(_src, vectors[i]);
                        // note that we use local indexes here!
                        queue.Enqueue(i, distance);
                    }

                    candidates.Clear();

                    while (candidates.Count <= searchState.Options.NumberOfEdges &&
                           queue.TryDequeue(out var cur, out var distance))
                    {
                        bool match = true;
                        foreach (var alternativeIndex in candidates)
                        {
                            var curDist = searchState.Distance(vectors[cur], vectors[alternativeIndex]);
                            // there is already an item in the result that is *closer* to the current
                            // node than the target node, so no need to add it
                            if (curDist < distance)
                            {
                                match = false;
                                break;
                            }
                        }

                        if (match)
                        {
                            candidates.Add(cur);
                        }
                    }

                    for (int i = 0; i < candidates.Count; i++)
                    {
                        // turn the local indexing into a global one
                        candidates[i] = indexes[candidates[i]];
                    }

                    queue.Clear();
                }
            }
            private sealed class ProcessEdgesWorker(NodePlacementRunner runner) : WorkItem(runner)
            {
                private UnmanagedSpan _vector;
                public float LowerBound;

                public void Reset(UnmanagedSpan vector, float lowerBound, int currentNodeIndex, int level)
                {
                    _vector = vector;
                    LowerBound = lowerBound;
                    CurrentNodeIndex = currentNodeIndex;
                    Level = level;
                }

                protected override void DoWork()
                {
                    var searchState = Owner._searchState;
                    var indexes = Owner._indexes;
                    var vectors = Owner._vectors;
                    var nearestEdgesQ = Owner._nearestEdgesQ;
                    var candidatesQ = Owner._candidatesQ;
                    var lowerBound  = LowerBound;
                    
                    int numberOfCandidates = searchState.Options.NumberOfCandidates;
                    for (int i = 0; i < indexes.Count; i++)
                    {
                        var nextIndex = indexes[i];
                        Debug.Assert(searchState.Nodes[nextIndex].EdgesPerLevel.Count > Level); 
                   
                        float nextDist = -searchState.Distance(_vector, vectors[i]);
                        if (nearestEdgesQ.Count < numberOfCandidates)
                        {
                            candidatesQ.Enqueue(nextIndex, -nextDist);
                            nearestEdgesQ.Enqueue(nextIndex, nextDist);
                        }
                        else if (lowerBound < nextDist)
                        {
                            candidatesQ.Enqueue(nextIndex, -nextDist);
                            nearestEdgesQ.EnqueueDequeue(nextIndex, nextDist);
                        }
                        else
                        {
                            continue;
                        }

                        Debug.Assert(candidatesQ.Count > 0);
                        nearestEdgesQ.TryPeek(out _, out lowerBound);
                    }
                    LowerBound = lowerBound;
                }
                
            }

            private sealed class FindNearestWorker(NodePlacementRunner runner) : WorkItem(runner)
            {
                private UnmanagedSpan _from;
                public float Distance;

                public void Reset(UnmanagedSpan from, int currentNodeIndex, int level)
                {
                    _from = from;
                    Distance = float.MaxValue;
                    CurrentNodeIndex = currentNodeIndex;
                    Level = level;
                }

                protected override void DoWork()
                {
                    var indexes = Owner._indexes;
                    var vectors = Owner._vectors;
                    var searchState = Owner._searchState;
                    
                    for (var i = 0; i < indexes.Count; i++)
                    {
                        var edgeIdx = indexes[i];
                        var curDist = searchState.Distance(_from, vectors[i]);
                        if (curDist >= Distance || double.IsNaN(curDist))
                            continue;
                        Distance = curDist;
                        CurrentNodeIndex = edgeIdx;
                    }
                }
            }
            
            private int GetLevelForNewNode(int maxLevel)
            {
                // Use the level assignment formula from the original HNSW paper.
                // Most nodes stay at level 0 where they form a dense, detailed graph
                // that captures fine-grained neighborhood relationships. Only a few
                // nodes get promoted to upper levels, which act as sparse long-range
                // shortcuts. During search, the algorithm quickly descends through
                // these thin upper layers to find a good entry region, then switches
                // to the dense level 0 to refine the actual nearest neighbors.
                // If promotion were too aggressive (e.g. a 50% coin flip), half the
                // nodes would reach level 1, a quarter level 2, and so on. The upper
                // layers would become crowded with nodes and edges, making insertion
                // and search spend most of their time navigating dense upper levels
                // instead of quickly skipping down to where the real work happens.
                int m = _searchState.Options.NumberOfEdges;
                double mL = 1.0 / Math.Log(m);
                double r = parent.Random.NextDouble();
                // Avoid log(0)
                if (r == 0.0) r = double.Epsilon;
                int level = (int)(-Math.Log(r) * mL);
                return Math.Min(level, maxLevel);
            }

            /// <summary>
            /// LLT-thread half of the former AfterPreloading. Performs only the steps that
            /// touch <see cref="SearchState.Llt"/> (and therefore must stay single-threaded):
            /// allocating edge-list capacity and, when the EdgesIndexesPerLevel mirror is
            /// stale, rebuilding it while force-loading any edge whose vector is still lazy.
            /// The bitmap + per-edge _indexes/_vectors fill loop is now in
            /// <see cref="PopulateWorkListsOnWorker"/>, which the WorkItem.Execute hook runs
            /// on a ThreadPool worker before <see cref="WorkItem.DoWork"/>.
            ///
            /// We always return true now (modulo the -1 sentinel for "use the existing
            /// _indexes from the previous yield") — the worker decides whether to call
            /// DoWork based on the post-fill _indexes count.
            /// </summary>
            public bool PrepareEdgesOnLLT(int currentNodeIndex, int level)
            {
                if (currentNodeIndex is -1)
                    return _indexes.Count > 0;

                ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);

                // The slow path runs RegisterForPreloading first, which sizes both lists.
                // The all-in-memory fast path skips that step, so we must guarantee the slot
                // exists before we ref into it. SetCapacity is a no-op when already sized.
                n.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, level + 1);
                n.EdgesIndexesPerLevel.SetCapacity(_searchState.Llt.Allocator, level + 1);

                ref var edgesList = ref n.EdgesPerLevel[level];
                ref var edgesIndexes = ref n.EdgesIndexesPerLevel[level];
                if (edgesIndexes.Count != edgesList.Count)
                {
                    // Mirror is stale: rebuild edgesIndexes AND, in the same pass, force-load any
                    // edge whose vector is still lazy. This is the only path that introduces
                    // freshly cache-resolved nodes (CopyNodeFromCache leaves _vectorSpan default),
                    // so once we walk it, every entry in edgesIndexes references a Node with
                    // VectorLoaded=true. _vectorSpan never resets, so subsequent calls on this
                    // (node, level) keep that invariant — letting us skip the per-edge VectorLoaded
                    // sweep entirely on the mirror-in-sync path. Profile (2026-05-09 split) had
                    // that sweep at 6.4 s / 15 s of LLT exclusive (43 %) while only ~2 % of calls
                    // actually triggered the rebuild.
                    edgesIndexes.ResetAndEnsureCapacity(_searchState.Llt.Allocator, edgesList.Count);
                    foreach (var nodeId in edgesList)
                    {
                        int idx = _searchState.GetNodeIndexById(nodeId);
                        edgesIndexes.AddUnsafe(idx);
                        ref var edge = ref _searchState.GetNodeByIndex(idx);
                        if (edge.VectorLoaded is false)
                            _ = edge.GetVectorUnmanagedSpan(_searchState);
                    }
                }

                // Snapshot the now-in-sync edge indexes into the per-task buffer ON THE LLT THREAD.
                // The worker reads this private copy in PopulateWorkListsOnWorker instead of the shared
                // EdgesIndexesPerLevel native buffer, so the LLT thread is free to grow/realloc that
                // buffer in later rounds without racing the worker (RavenDB-26809). The copy is a
                // consistent point-in-time view, so the worker never observes a torn read.
                _edgeIndexSnapshot.Clear();
                _edgeIndexSnapshot.AddRange(edgesIndexes.ToSpan());

                return true; // always dispatch; worker decides via _indexes.Count after fill
            }

            /// <summary>
            /// Worker-thread half of the former AfterPreloading. Walks the edges of
            /// <paramref name="currentNodeIndex"/> at <paramref name="level"/>, applying the
            /// per-task visited bitmap and populating <see cref="_indexes"/> / <see cref="_vectors"/>
            /// for the upcoming WorkItem.DoWork call. All state mutated here lives on the
            /// owning NodePlacement (per-task, never shared) — the SearchState reads are
            /// either field reads on already-loaded nodes (guaranteed by PrepareEdgesOnLLT)
            /// or by-index lookups into the shared <see cref="SearchState.Nodes"/> array,
            /// which is safe to read concurrently while the LLT thread is parked in dispatch.
            /// Returns true iff DoWork has anything to compute.
            /// </summary>
            public bool PopulateWorkListsOnWorker(int currentNodeIndex, int level)
            {
                if (currentNodeIndex is -1)
                    return _indexes.Count > 0;

                ref var n = ref _searchState.GetNodeByIndex(currentNodeIndex);
                _indexes.Clear();
                _vectors.Clear();
                if (MarkVisited(currentNodeIndex))
                {
                    _indexes.Add(currentNodeIndex);
                    _vectors.Add(n.GetVectorUnmanagedSpan(_searchState));
                }

                // Read the per-task snapshot captured on the LLT thread in PrepareEdgesOnLLT, NOT the
                // shared EdgesIndexesPerLevel native buffer: that buffer's storage can be freed and
                // reallocated by the LLT thread in a later round while this worker is still running
                // (use-after-free, RavenDB-26809).
                foreach (var idx in _edgeIndexSnapshot)
                {
                    if (MarkVisited(idx) is false)
                        continue;
                    _indexes.Add(idx);
                    ref var edge = ref _searchState.GetNodeByIndex(idx);
                    _vectors.Add(edge.GetVectorUnmanagedSpan(_searchState));
                }

                return _indexes.Count > 0;
            }
        }

        /// <summary>
        /// This works opposite to how you'll usually think about such runners.
        /// It is running everything in a _single_ threaded (because it uses the single threaded transaction)
        /// and offload computational work to the thread pool, this is done using the NodePlacement yielding
        /// whenever it wants to offload a computation, and the runner is then taking care of running the code,
        /// 
        /// </summary>
        private class NodePlacementRunner
        {
            private readonly int _activeTasksCount;
            private int _completed;
            private readonly ManualResetEventSlim _ready = new();
            private readonly ConcurrentQueue<IEnumerator<WorkItem>> _placementTasks = [];
            private readonly ConcurrentQueue<(Exception Error, IEnumerator<WorkItem> It)> _placementErrors = [];
            private readonly List<WorkItem> _items = [];
            private readonly SearchState _searchState;
            private readonly CancellationTokenSource _errorCts = new();
            private readonly CancellationTokenSource _mainCts;
            private readonly List<Exception> _errors = [];
            private readonly LinkedList<int> _inFlightIndexes = [];

            // Latches to true once an iteration completes with nothing left to preload, meaning
            // every node touched so far is resident. From that point we skip the RegisterForPreloading
            // scan (its O(items * edgesPerNode) VectorLoaded / TryGetNodeById sweep) and call
            // AfterPreloading directly. The fast path stays correct even if the heuristic is wrong
            // for a future item: GetVectorUnmanagedSpan falls back to a single-vector load on miss,
            // so the worst case is degrading to one cold load instead of a batched one.
            private bool _allVectorsInMemory;

            public bool IsCancelled => _mainCts.IsCancellationRequested;
            

            public void AddInFlight(LinkedListNode<int> node)
            {
                _inFlightIndexes.AddLast(node);
            }

            public void RemoveInFlight(LinkedListNode<int> node)
            {
                _inFlightIndexes.Remove(node);
            }

            public NodePlacementRunner(Registration parent, int activeTasksCount, CancellationToken token)
            {
                _mainCts = CancellationTokenSource.CreateLinkedTokenSource(token, _errorCts.Token);
                _activeTasksCount = activeTasksCount;
                _searchState = parent._searchState;

                for (int i = 0; i < activeTasksCount; i++)
                {
                    Enqueue(new NodePlacement(parent, this).Process().GetEnumerator());
                }
            }

            
            public void Run()
            {
                List<long> batch = [];
                while (true)
                {
                    _ready.Wait();
                    _ready.Reset();
                    
                    while(_placementTasks.TryDequeue(out var it))
                    {
                        if (it.MoveNext())
                        {
                            WorkItem current = it.Current!;
                            current.Iterator = it;
                            _items.Add(current);
                        }
                        else
                        {
                            it.Dispose();
                        }
                    }

                    while (_placementErrors.TryDequeue(out var cur))
                    {
                        HandleError(cur.Error, cur.It);
                    }

                    if (_completed == _activeTasksCount)
                    {
                        if(_errors.Count > 0)
                            throw new AggregateException(_errors);
                        if (_errorCts.IsCancellationRequested == false && _mainCts.IsCancellationRequested)
                        {
                            // If _mainCts is canceled and _errorCts is not, then we need to throw 
                            // to indicate that we're done due to operation cancellation!
                            _mainCts.Token.ThrowIfCancellationRequested();
                        }
                            
                        return; // done
                    }
                    
                    if (_allVectorsInMemory)
                    {
                        // Fast path: every previously touched node is resident, so the bulk preload
                        // scan has nothing to find. Run the LLT-only edge prep here and always
                        // dispatch — the worker's PopulateWorkListsOnWorker fills the visited
                        // bitmap + _indexes/_vectors and decides whether DoWork has anything to
                        // compute.
                        for (int index = 0; index < _items.Count; index++)
                        {
                            WorkItem item = _items[index];
                            item.Owner.PrepareEdgesOnLLT(item.CurrentNodeIndex, item.Level);
                            ThreadPool.UnsafeQueueUserWorkItem(item, preferLocal: false);
                        }

                        _items.Clear();
                        continue;
                    }

                    // we executed all that we could, now let's check if we have
                    // any edges to load that we can do in bulk
                    batch.Clear();
                    for (int index = 0; index < _items.Count; index++)
                    {
                        WorkItem item = _items[index];
                        if (item.RegisterForPreloading(_searchState, batch))
                            continue;

                        // we can run this directly, since there is nothing to preload
                        _items[index] = null; // skip it in the rest of the process
                        item.Owner.PrepareEdgesOnLLT(item.CurrentNodeIndex, item.Level);
                        ThreadPool.UnsafeQueueUserWorkItem(item, preferLocal: false);
                    }

                    var batchSpan = CollectionsMarshal.AsSpan(batch);
                    var used = Sorting.SortAndRemoveDuplicates(batchSpan);
                    if (used > 0)
                    {
                        _searchState.PreloadNodesVectors(batchSpan[..used]);
                    }
                    else
                    {
                        // The whole working set is resident, switch to the fast path on the next iteration.
                        _allVectorsInMemory = true;
                    }

                    foreach (var item in _items)
                    {
                        if (item is null) continue;

                        item.Owner.PrepareEdgesOnLLT(item.CurrentNodeIndex, item.Level);
                        ThreadPool.UnsafeQueueUserWorkItem(item, preferLocal: false);
                    }

                    _items.Clear();
                }
            }
            
            
            private void HandleError(Exception error, IEnumerator<WorkItem> it)
            {
                // force all pending work to stop now, instead of when it is all done
                _errorCts.Cancel();
                _errors.Add(error);
                try
                {
                    it.Dispose();
                }
                catch (Exception e)
                {
                    _errors.Add(e);
                }
            }

            public void Enqueue(IEnumerator<WorkItem> it)
            { 
                _placementTasks.Enqueue(it);
                _ready.Set();
            }

            public void Error(IEnumerator<WorkItem> it, Exception exception)
            {
                _placementErrors.Enqueue((exception, it));
                _ready.Set();
            }

            public void Done()
            {
                _completed++;
            }
            
            public int GetInFlightIndexes(LinkedListNode<int> n, Span<int> buffer)
            {
                var index = 0;
                var cur = n.Previous;
                while(cur != null && index < buffer.Length)
                {
                    buffer[index++] = cur.Value;
                    cur = cur.Previous;
                }

                return index;
            }
        }
        
        private abstract class WorkItem(NodePlacementRunner runner) : IThreadPoolWorkItem
        {
            public NodePlacement Owner;
            public IEnumerator<WorkItem> Iterator;

            protected abstract void DoWork();

            void IThreadPoolWorkItem.Execute()
            {
                try
                {
                    // PopulateWorkListsOnWorker performs the bitmap-visit + per-edge
                    // _indexes/_vectors fill that used to be in AfterPreloading on the LLT
                    // thread. It returns false when there's nothing to compute (all edges
                    // already visited this round) — in which case we skip DoWork and just
                    // re-yield the iterator.
                    if (Owner.PopulateWorkListsOnWorker(CurrentNodeIndex, Level))
                    {
                        DoWork();
                    }

                    runner.Enqueue(Iterator);
                }
                catch (Exception e)
                {
                    runner.Error(Iterator, e);
                }
            }

            public int CurrentNodeIndex;
            public int Level;

            /// <summary>
            /// This scans over all the items that we _want_ to load and check if
            /// their vectors were already loaded. If not, it registers them to be loaded
            /// in a batch manner.
            ///
            /// It may find out that there is no actual work to be done here, in which case
            /// the work item can start immediately.
            /// </summary>
            public bool RegisterForPreloading(SearchState searchState, List<long> batch)
            {
                if (CurrentNodeIndex is -1)
                    return false;
                
                int old = batch.Count;
                ref var n = ref searchState.GetNodeByIndex(CurrentNodeIndex);
                if (n.VectorLoaded is false)
                    batch.Add(n.NodeId);
                
                n.EdgesPerLevel.SetCapacity(searchState.Llt.Allocator, Level + 1);
                n.EdgesIndexesPerLevel.SetCapacity(searchState.Llt.Allocator, Level + 1);

                ref var edgesList = ref n.EdgesPerLevel[Level];
                ref var edgesIndexes = ref n.EdgesIndexesPerLevel[Level];
                // turns out that the checks for the node id -> index are really expensive
                // so we try to cache them
                if (edgesIndexes.Count != edgesList.Count)
                {
                    edgesIndexes.ResetAndEnsureCapacity(searchState.Llt.Allocator, edgesList.Count);
                    for (int i = 0; i < edgesList.Count; i++)
                    {
                        var nodeId = edgesList[i];
                        if (searchState.TryGetNodeById(nodeId, out var nodeIndex))
                        {
                            edgesIndexes.AddUnsafe(nodeIndex);
                            continue;
                        }
                        // we add it to be pre-loaded
                        batch.Add(nodeId);
                        // not that we did NOT add to the edges, so the _next_ time
                        // we run, we'll re-do the whole check and find the node index
                    }
                }
                
                for (int i = 0; i < edgesIndexes.Count; i++)
                {
                    int index = edgesIndexes[i];
                    if (searchState.Nodes[index].VectorLoaded)
                        continue;
                    batch.Add(edgesList[i]);
                }
                return old != batch.Count;
            }
        }
    }
}
