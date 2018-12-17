using System;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Logging;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class MapReduceIndexingContext : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MapReduceResultsStore>("MapReduceIndexingContext");

        internal static Slice LastMapResultIdKey;

        public FixedSizeTree DocumentMapEntries;

        public Tree MapPhaseTree;
        public Tree ReducePhaseTree;
        
        public FixedSizeTree ResultsStoreTypes;
        public Dictionary<ulong, MapReduceResultsStore> StoreByReduceKeyHash = new Dictionary<ulong, MapReduceResultsStore>(NumericEqualityComparer.BoxedInstanceUInt64);
        public Dictionary<string, long> ProcessedDocEtags = new Dictionary<string, long>();
        public Dictionary<string, long> ProcessedTombstoneEtags = new Dictionary<string, long>();

        public event Action<long> PageModifiedInReduceTree;

        public void OnPageModifiedInReduceTree(long page)
        {
            PageModifiedInReduceTree?.Invoke(page);
        }

        public long NextMapResultId;

        static MapReduceIndexingContext()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "__raven/map-reduce/#next-map-result-id", ByteStringType.Immutable, out LastMapResultIdKey);
            }
        }

        public void Dispose()
        {
            try
            {
                StoreNextMapResultId();
            }
            finally
            {
                DocumentMapEntries?.Dispose();
                DocumentMapEntries = null;
                MapPhaseTree = null;
                ReducePhaseTree = null;
                PageModifiedInReduceTree = null;
                ProcessedDocEtags.Clear();
                ProcessedTombstoneEtags.Clear();
                StoreByReduceKeyHash.Clear();
            }
        }

        public unsafe void StoreNextMapResultId()
        {
            if (MapPhaseTree.Llt.Environment.Options.IsCatastrophicFailureSet)
                return; // avoid re-throwing it

            try
            {
                using (MapPhaseTree.DirectAdd(LastMapResultIdKey, sizeof(long), out byte* ptr))
                    *(long*)ptr = NextMapResultId;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to store next map result id", e);

                throw;
            }
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
