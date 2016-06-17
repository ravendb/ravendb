using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Voron.Data.BTrees;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class MapReduceIndexingContext : IDisposable
    {
        private readonly Queue<long> _idsOfDeletedEntries = new Queue<long>();

        public Tree MapEntries;
        public Dictionary<ulong, ReduceKeyState> StateByReduceKeyHash = new Dictionary<ulong, ReduceKeyState>();
        public Dictionary<string, long> ProcessedDocEtags = new Dictionary<string, long>();
        public Dictionary<string, long> ProcessedTombstoneEtags = new Dictionary<string, long>();
        
        internal long LastMapResultId = -1; // TODO arek - initialize on index startup

        public void Dispose()
        {
            MapEntries = null;
            ProcessedDocEtags.Clear();
            ProcessedTombstoneEtags.Clear();
            StateByReduceKeyHash.Clear();
            _idsOfDeletedEntries.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EntryDeleted(long deletedEntryId)
        {
            _idsOfDeletedEntries.Enqueue(deletedEntryId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetNextIdentifier()
        {
            return _idsOfDeletedEntries.Count > 1 ? _idsOfDeletedEntries.Dequeue() : ++LastMapResultId;
        }
    }
}