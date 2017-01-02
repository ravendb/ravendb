using System;
using System.Collections.Generic;
using Sparrow;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class MapReduceIndexingContext : IDisposable
    {
        internal static Slice LastMapResultIdKey;

        public FixedSizeTree DocumentMapEntries;

        public Tree MapPhaseTree;
        public Tree ReducePhaseTree;
        
        public FixedSizeTree ResultsStoreTypes;
        public Dictionary<ulong, MapReduceResultsStore> StoreByReduceKeyHash = new Dictionary<ulong, MapReduceResultsStore>(NumericEqualityComparer.Instance);
        public Dictionary<string, long> ProcessedDocEtags = new Dictionary<string, long>();
        public Dictionary<string, long> ProcessedTombstoneEtags = new Dictionary<string, long>();
        
        public long NextMapResultId;

        static MapReduceIndexingContext()
        {
             Slice.From(StorageEnvironment.LabelsContext, "__raven/map-reduce/#next-map-result-id", ByteStringType.Immutable, out LastMapResultIdKey);
        }

        public void Dispose()
        {
            StoreNextMapResultId();
            DocumentMapEntries?.Dispose();
            DocumentMapEntries = null;
            MapPhaseTree = null;
            ReducePhaseTree = null;
            ProcessedDocEtags.Clear();
            ProcessedTombstoneEtags.Clear();
            StoreByReduceKeyHash.Clear();
        }

        public unsafe void StoreNextMapResultId()
        {
            *(long*)MapPhaseTree.DirectAdd(LastMapResultIdKey, sizeof(long)) = NextMapResultId;
        }

        public unsafe void Initialize(Tree mapEntriesTree)
        {
            var read = mapEntriesTree.Read(LastMapResultIdKey);

            if (read == null)
                return;

            NextMapResultId = *(long*)read.Reader.Base;
        }
    }
}