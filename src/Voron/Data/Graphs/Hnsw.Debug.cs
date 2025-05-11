using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Compression;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Constants = Voron.Global.Constants;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    public record NodeForDebug(
        long NodeId,
        long[] Entries,
        (long NodeId, float Distance)[][] EdgesByLevel
    );
    
    public static IEnumerable<NodeForDebug> IterateNodes(LowLevelTransaction llt, string name)
    {
        var searchState = new SearchState(llt, name);
        for (long nodeId = 1; nodeId <= searchState.Options.CountOfVectors; nodeId++)
        {
            var node = searchState.GetNodeById(nodeId);
            int nodeIndex = searchState.GetNodeIndexById(nodeId);
            long[] entries = GetEntries(llt, node.PostingListId);
            var edgesByLevel = new (long NodeId, float Distance)[node.EdgesPerLevel.Count][];
            for (int i = 0; i < node.EdgesPerLevel.Count; i++)
            {
                edgesByLevel[i] = new (long NodeId, float Distance)[node.EdgesPerLevel[i].Count];
                for (int j = 0; j <  node.EdgesPerLevel[i].Count; j++)
                {
                    long id = node.EdgesPerLevel[i][j];
                    int index = searchState.GetNodeIndexById(id);
                    edgesByLevel[i][j] = (id, searchState.Distance(ReadOnlySpan<byte>.Empty, nodeIndex, index));
                }
            }
            yield return new NodeForDebug(nodeId, entries, edgesByLevel);
        }
    }

    public static long[] GetEntries(LowLevelTransaction llt,long postingListId)
    {
        long rawPostingListId = postingListId & Constants.Graphs.VectorId.ContainerType;
        long[] result;
        switch (postingListId & Constants.Graphs.VectorId.EnsureIsSingleMask)
        {
            case Constants.Graphs.VectorId.Tombstone:
                result= [];
                break;
            case Constants.Graphs.VectorId.Single:
                result = [rawPostingListId];
                break;
            case Constants.Graphs.VectorId.SmallPostingList:
            {
                var list = new ContextBoundNativeList<long>(llt.Allocator);
                FastPForDecoder decoder = new();
                SearchState.ReadPostingList(llt, rawPostingListId, ref list, ref decoder, out var size);
                result = list.ToSpan().ToArray();
                list.Dispose();
                break;
            }
            case Constants.Graphs.VectorId.PostingList:
            {
                var setStateSpan = Container.GetReadOnly(llt, rawPostingListId);
                ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                var postingList = new PostingList(llt, Slices.Empty, setState);
                result = new long[(int)Math.Min(postingList.State.NumberOfEntries, 16)];
                var it = postingList.Iterate();
                it.Fill(result, out _);
                break;
            }
            default:
                throw new NotSupportedException($"Got unknown {nameof(postingListId)} type: {postingListId}");
        }
        Registration.InternalEntryIdToEntryId(result);
        return result;
    }
    
    private static long GetEntryId(LowLevelTransaction llt,long postingListId)
    {
        switch (postingListId & Constants.Graphs.VectorId.EnsureIsSingleMask)
        {
            case Constants.Graphs.VectorId.Tombstone:
                return 0;
            case Constants.Graphs.VectorId.Single:
                return postingListId & Constants.Graphs.VectorId.ContainerType;
            case 0b10:
                return postingListId & Constants.Graphs.VectorId.ContainerType;
            case 0b11:
                return postingListId & Constants.Graphs.VectorId.ContainerType;
        }

        throw new NotSupportedException($"Got unknown {nameof(postingListId)} type: {postingListId}");
    }

    public static void RenderAndShow(LowLevelTransaction llt, string name, Span<byte> vector)
    {
        using (Slice.From(llt.Allocator, name, out var slice))
        {
            RenderAndShow(llt, slice, vector);
        }
    }
    
    public static void RenderAndShow(LowLevelTransaction llt, Slice name, Span<byte> vector)
    {
        var searchState = new SearchState(llt, name);
        string fileName = Path.GetTempFileName() + ".html";
        using (var f = File.CreateText(fileName))
        {
            f.WriteLine(@"<html><style>
/* Basic table styling */
table {
    width: 100%;
    border-collapse: collapse;
}
/* Style for table headers */
th {
    background-color: #f2f2f2;
    color: #333;
    padding: 10px;
    text-align: left;
    border-bottom: 2px solid #ddd;
}
th.result {
    background-color: Violet;
}
th.path {
    background-color: aqua;
}
/* Style for table cells */
td {
    padding: 10px;
    border-bottom: 1px solid #ddd;
}
/* Alternate row colors for better readability */
tr:nth-child(even) {
    background-color: #f9f9f9;
}
/* Add some padding and border to the table */
table, th, td {
    border: 1px solid #ddd;
}

</style><body>");

            var path = new NativeList<int>();
            path.EnsureCapacityFor(llt.Allocator, searchState.Options.MaxLevel +1);
            var edges = new NativeList<int>();
            edges.EnsureCapacityFor(llt.Allocator, 16);
            searchState.SearchNearestAcrossLevels(vector, -1, searchState.Options.MaxLevel,  ref path);
            searchState.NearestEdges(path[0], -1, vector, 0, 8, ref edges, SearchState.NearestEdgesFlags.StartingPointAsEdge);
            
            for (int level = searchState.Options.MaxLevel - 1; level >= 0; level--)
            {
                f.WriteLine($"<h1>Level: {level}</h1>");
                f.WriteLine("<table><tr>");
                int cols = 0;
                for (int j = 1; j <= searchState.Options.CountOfVectors; j++)
                {
                    var nodeIdx = searchState.GetNodeIndexById(j);
                    ref var n = ref searchState.Nodes[nodeIdx];
                    if (level >= n.EdgesPerLevel.Count)
                        continue;

                    var dist = searchState.Distance(vector, -1, nodeIdx);
                    var isPath = path[level] == nodeIdx ? "path" : "";
                    var isResult =  level == 0 && edges.Items.Contains(nodeIdx) ? "result": "";
                    var nextId = level == 0 ? (edges.Items.Contains(nodeIdx) ?"***": "") : $"N_{path[level - 1]}_{level - 1}";
                    f.WriteLine($"<td> <table id='N_{j}_{level}'><tr><th class='{isPath} {isResult}'>N_{j}_{level} - {GetEntryId(llt,n.PostingListId)}</th>" +
                                $"<th>{n.EdgesPerLevel[level].Count}</th><th>{dist} (<a href='#{nextId}'>{nextId}</a>)</th></tr><tr>");
                    foreach (var to in n.EdgesPerLevel[level])
                    {
                        dist = searchState.Distance(Span<byte>.Empty, nodeIdx, searchState.GetNodeIndexById(to));
                        var srcDist = searchState.Distance(vector, -1, searchState.GetNodeIndexById(to));
                        var id = $"N_{to}_{Math.Max(0, level-1)}";
                     
                        f.WriteLine($"<tr><td><a href='#{id}'>{id}</a></td><td>{dist}</td><td>{srcDist}</td></tr>");
                    }
                    f.WriteLine("</table></td>");
                    if (++cols == 8)
                    {
                        f.WriteLine("</tr><tr>");
                        cols = 0;
                    }
                }

                f.WriteLine("</tr></table>");
            }

            // for (long j = 1; j <= searchState.Options.CountOfVectors; j++)
            // {
            //     ref var n = ref searchState.GetNodeById(j);
            //     for (int i = 1; i < n.NeighborsPerLevel.Count; i++)
            //     {
            //         f.WriteLine($"\tN_{j}_{i - 1} -- N_{j}_{i};");
            //     }
            // }

            f.WriteLine("</body></html>");
        }

        DebugStuff.OpenBrowser(fileName);
    }
}
