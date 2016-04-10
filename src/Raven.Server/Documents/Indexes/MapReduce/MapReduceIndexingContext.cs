using System;
using System.Collections.Generic;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class MapReduceIndexingContext : IDisposable
    {
        public Table MapEntriesTable;
        public Dictionary<ulong, ReduceKeyState> StateByReduceKeyHash = new Dictionary<ulong, ReduceKeyState>();
        public void Dispose()
        {
            MapEntriesTable = null;
            StateByReduceKeyHash.Clear();
        }
    }
}