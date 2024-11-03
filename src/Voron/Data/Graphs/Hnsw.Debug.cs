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
using Voron.Debugging;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    private static long GetPostingListCount(LowLevelTransaction llt,long postingListId)
    {
        switch (postingListId & 0b11)
        {
            case 0:
                return 0;
            case 0b01:
                return 1;
            case 0b10:
                var item = Container.Get(llt, postingListId & ~0b11);
                return VariableSizeEncoding.Read<int>(item.Address, out _);
            case 0b11:
                throw new NotImplementedException();
        }

        throw new NotSupportedException();
    }
    
    private static long GetEntryId(LowLevelTransaction llt,long postingListId)
    {
        switch (postingListId & 0b11)
        {
            case 0:
                return 0;
            case 0b01:
                return postingListId & ~0b11;
            case 0b10:
                return -1;
            case 0b11:
                return -2;
        }

        throw new NotSupportedException();
    }


    public static void RenderAndShow(LowLevelTransaction llt, long graphId, Span<byte> vector)
    {
        var searchState = new SearchState(llt, graphId);
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
            searchState.SearchNearestAcrossLevels(vector, ref Unsafe.NullRef<Node>(), searchState.Options.MaxLevel,  ref path);
            searchState.NearestEdges(path[0], 0, 8, vector, ref Unsafe.NullRef<Node>(), ref edges, true);
            
            for (int level = searchState.Options.MaxLevel - 1; level >= 0; level--)
            {
                f.WriteLine($"<h1>Level: {level}</h1>");
                f.WriteLine("<table><tr>");
                int cols = 0;
                for (int j = 1; j <= searchState.Options.CountOfVectors; j++)
                {
                    ref var n = ref searchState.GetNodeById(j);
                    if (level >= n.EdgesPerLevel.Count)
                        continue;

                    var dist = searchState.Distance(vector, ref Unsafe.NullRef<Node>(), ref n);
                    var isPath = path[level] == j ? "path" : "";
                    var isResult =  level == 0 && edges.Items.Contains(j) ? "result": "";
                    var nextId = level == 0 ? (edges.Items.Contains(j) ?"***": "") : $"N_{path[level - 1]}_{level - 1}";
                    f.WriteLine($"<td> <table id='N_{j}_{level}'><tr><th class='{isPath} {isResult}'>N_{j}_{level} - {GetEntryId(llt,n.PostingListId)}</th>" +
                                $"<th>{n.EdgesPerLevel[level].Count}</th><th>{dist} (<a href='#{nextId}'>{nextId}</a>)</th></tr><tr>");
                    foreach (var to in n.EdgesPerLevel[level])
                    {
                        dist = searchState.Distance(Span<byte>.Empty, ref n, ref searchState.GetNodeById(to));
                        var srcDist = searchState.Distance(vector, ref Unsafe.NullRef<Node>(), ref searchState.GetNodeById(to));
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
