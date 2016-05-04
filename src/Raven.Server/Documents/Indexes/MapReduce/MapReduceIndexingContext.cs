using System;
using System.Collections.Generic;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class MapReduceIndexingContext : IDisposable
    {
        public Table MapEntriesTable;
        public Dictionary<ulong, ReduceKeyState> StateByReduceKeyHash = new Dictionary<ulong, ReduceKeyState>();
        public Dictionary<string, long> ProcessedDocEtags = new Dictionary<string, long>();
        public Dictionary<string, long> ProcessedTombstoneEtags = new Dictionary<string, long>();

        public void Dispose()
        {
            MapEntriesTable = null;
            ProcessedDocEtags.Clear();
            ProcessedTombstoneEtags.Clear();
            StateByReduceKeyHash.Clear();
        }
    }
}