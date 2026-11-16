using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Server;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public partial class Registration
    {
        // The lists that back-links put over their cap while the batch is placed, by level. Touched only on the LLT thread.
        private readonly List<HashSet<int>> _overfullLists = [];

        private bool SelectsListsAfterPlacement => _forTestingPurposes?.SelectsListsOnOverflow != true;

        private void AddOverfullList(int nodeIndex, int level)
        {
            while (_overfullLists.Count <= level)
                _overfullLists.Add([]);
            _overfullLists[level].Add(nodeIndex);
        }

        private void SelectOverfullLists(CancellationToken token)
        {
            using var selection = new OverfullListSelection(_searchState);
            for (int level = 0; level < _overfullLists.Count; level++)
            {
                if (_overfullLists[level].Count == 0)
                    continue;

                selection.Run(_overfullLists[level], level, token);
                _overfullLists[level].Clear();
            }
        }

        // Selects, once, each list of one level that went over its cap during the batch. The heuristic runs on the lists in
        // parallel; reading and writing them stay on the LLT thread.
        private sealed class OverfullListSelection(SearchState searchState) : IDisposable
        {
            private struct OverfullList
            {
                public int Node;
                public UnmanagedSpan Vector;
                // The edges of the list, as node indexes in _edges and vectors in _edgeVectors, start here.
                public int EdgesStart;
                public int EdgesCount;
                // The first Selected positions of its selection are the edges it keeps.
                public int Selected;
            }

            private readonly ByteStringContext _allocator = searchState.Llt.Allocator;
            private NativeList<OverfullList> _lists;
            private NativeList<int> _edges;
            private NativeList<UnmanagedSpan> _edgeVectors;
            // _maxEdges slots for each list: the positions, in the edges of the list, of the edges it keeps.
            private NativeList<int> _selection;
            // For each node index, the number of the last list that added it, counted from 1: 0, the value of a new entry, means none.
            private NativeList<int> _listOfEdge;
            private NativeList<long> _ids;
            private int _level;
            private int _maxEdges;

            public void Run(HashSet<int> overfull, int level, CancellationToken token)
            {
                _level = level;
                _maxEdges = searchState.Options.MaxEdgesAtLevel(level);
                Gather(overfull);
                if (_lists.Count == 0)
                    return;

                Select(token);

                for (int i = 0; i < _lists.Count; i++)
                    Write(i);
            }

            private Span<int> EdgesOf(in OverfullList list) => _edges.ToSpan().Slice(list.EdgesStart, list.EdgesCount);

            private Span<int> SelectionOf(int list) => _selection.ToSpan().Slice(list * _maxEdges, _maxEdges);

            private void Gather(HashSet<int> overfull)
            {
                _lists.Clear();
                _edges.Clear();
                _edgeVectors.Clear();
                _listOfEdge.Clear();
                _listOfEdge.SetCapacity(_allocator, searchState.Nodes.Length);
                foreach (var nodeIndex in overfull)
                {
                    if (searchState.GetNodeByIndex(nodeIndex).EdgesPerLevel[_level].Count <= _maxEdges)
                        continue;

                    var list = new OverfullList { Node = nodeIndex, EdgesStart = _edges.Count };
                    AddEdgesOf(nodeIndex, _lists.Count + 1);
                    list.EdgesCount = _edges.Count - list.EdgesStart;
                    list.Vector = searchState.GetNodeByIndex(nodeIndex).GetVectorUnmanagedSpan(searchState);
                    _lists.Add(_allocator, list);
                }

                _edgeVectors.EnsureCapacityFor(_allocator, _edges.Count);
                foreach (var edge in _edges.ToSpan())
                    _edgeVectors.AddUnsafe(searchState.GetNodeByIndex(edge).GetVectorUnmanagedSpan(searchState));
            }

            private void AddEdgesOf(int nodeIndex, int listMark)
            {
                ref var node = ref searchState.GetNodeByIndex(nodeIndex);
                if (node.EdgesIndexesPerLevel.Count > _level && node.EdgesIndexesPerLevel[_level].Count == node.EdgesPerLevel[_level].Count)
                {
                    searchState.AssertEdgeIndexesMatchEdges(nodeIndex, _level);
                    foreach (var edgeIndex in node.EdgesIndexesPerLevel[_level].ToSpan())
                        AddEdge(nodeIndex, edgeIndex, listMark);
                    return;
                }

                // Reading a node may move the nodes, so the ids are copied first.
                _ids.ResetAndCopyFrom(_allocator, node.EdgesPerLevel[_level].ToSpan());
                foreach (var id in _ids.ToSpan())
                    AddEdge(nodeIndex, searchState.GetNodeIndexById(id), listMark);
            }

            private void AddEdge(int nodeIndex, int edgeIndex, int listMark)
            {
                if (edgeIndex == nodeIndex)
                    return;

                if (edgeIndex >= _listOfEdge.Count)
                    _listOfEdge.SetCapacity(_allocator, Math.Max(edgeIndex + 1, _listOfEdge.Count * 2));

                // A list may hold an edge twice, and keeps it once.
                ref int listOfEdge = ref _listOfEdge[edgeIndex];
                if (listOfEdge == listMark)
                    return;

                listOfEdge = listMark;
                _edges.Add(_allocator, edgeIndex);
            }

            private void Select(CancellationToken token)
            {
                _selection.Clear();
                _selection.SetCapacity(_allocator, _lists.Count * _maxEdges);
                int prunedFillLimit = searchState.Options.PrunedFillLimitAtLevel(_level);
                Parallel.For(0, _lists.Count, new ParallelOptions { CancellationToken = token }, () => new HeuristicScratch(), (i, _, scratch) =>
                {
                    ref var list = ref _lists[i];
                    var vectors = _edgeVectors.ToSpan().Slice(list.EdgesStart, list.EdgesCount);
                    list.Selected = scratch.Select(searchState, list.Vector, vectors, _maxEdges, prunedFillLimit, SelectionOf(i));
                    return scratch;
                }, scratch => scratch.Dispose());
            }

            private void Write(int index)
            {
                ref var list = ref _lists[index];
                var edges = EdgesOf(list);
                var kept = SelectionOf(index)[..list.Selected];
                // The selection holds positions in the edges of the list; the lists of a node hold node indexes and ids.
                for (int i = 0; i < kept.Length; i++)
                    kept[i] = edges[kept[i]];

                searchState.GetNodeByIndex(list.Node).SetEdges(searchState, _level, kept);
            }

            public void Dispose()
            {
                _lists.Dispose(_allocator);
                _edges.Dispose(_allocator);
                _edgeVectors.Dispose(_allocator);
                _selection.Dispose(_allocator);
                _listOfEdge.Dispose(_allocator);
                _ids.Dispose(_allocator);
            }
        }
    }
}
