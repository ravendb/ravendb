using System;
using System.Collections.Generic;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class MapReduceIndexingContext : IDisposable
    {
        public Table MapEntriesTable;
        public Dictionary<ulong, ReduceKeyState> StateByReduceKeyHash = new Dictionary<ulong, ReduceKeyState>();
        public Dictionary<string, long> LastEtags = new Dictionary<string, long>();

        public void Dispose()
        {
            MapEntriesTable = null;
            LastEtags.Clear();
            StateByReduceKeyHash.Clear();
        }
    }
}