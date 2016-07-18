using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron;
using Voron.Data.BTrees;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class MapReduceIndexingContext : IDisposable
    {
        internal Slice LastMapResultIdKey = Slice.From(StorageEnvironment.LabelsContext, "#LastMapResultId", ByteStringType.Immutable);

        private readonly Queue<long> _idsOfDeletedEntries = new Queue<long>();

        public Tree MapEntries;
        public Dictionary<ulong, ReduceKeyState> StateByReduceKeyHash = new Dictionary<ulong, ReduceKeyState>();
        public Dictionary<string, long> ProcessedDocEtags = new Dictionary<string, long>();
        public Dictionary<string, long> ProcessedTombstoneEtags = new Dictionary<string, long>();
        
        internal long LastMapResultId = -1;

        public void Dispose()
        {
            StoreLastMapResultId();

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

        public unsafe void StoreLastMapResultId()
        {
            *(long*)MapEntries.DirectAdd(LastMapResultIdKey, sizeof(long)) = LastMapResultId;
        }

        public unsafe void Initialize(Tree mapEntriesTree)
        {
            var read = mapEntriesTree.Read(LastMapResultIdKey);

            if (read == null)
                return;

            LastMapResultId = *(long*)read.Reader.Base;
        }
    }
}