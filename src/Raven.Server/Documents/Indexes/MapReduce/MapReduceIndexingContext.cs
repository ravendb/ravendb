using System;
using System.Collections.Generic;
using Raven.Server.Logging;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public sealed class MapReduceIndexingContext : IDisposable
    {
        private readonly Index _index;
        private RavenLogger _logger;

        private RavenLogger Logger
        {
            get => _logger ??= RavenLogManager.Instance.GetLoggerForIndex<MapReduceIndexingContext>(_index);
        }

        internal static Slice LastMapResultIdKey;

        public FixedSizeTree DocumentMapEntries;

        public Tree MapPhaseTree;
        public Tree ReducePhaseTree;
        
        public FixedSizeTree ResultsStoreTypes;
        public Dictionary<ulong, MapReduceResultsStore> StoreByReduceKeyHash = new();
        public Dictionary<string, long> ProcessedDocEtags = new();
        public Dictionary<string, long> ProcessedTombstoneEtags = new();
        public Dictionary<string, long> ProcessedTimeSeriesDeletedRangeEtags = new();
        public readonly HashSet<long> FreedPages = new();

        public List<MapEntry> MapEntries = new();

        public long NextMapResultId;

        private const int MaxRetainedMapEntries = 1024;

        private ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> _mapResultBuilder;

        static MapReduceIndexingContext()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "__raven/map-reduce/#next-map-result-id", ByteStringType.Immutable, out LastMapResultIdKey);
            }
        }

        public MapReduceIndexingContext(Index index)
        {
            _index = index ?? throw new ArgumentNullException(nameof(index));
        }

        public ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> GetMapResultBuilder(JsonOperationContext indexContext)
        {
            return _mapResultBuilder ??= new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(indexContext);
        }

        public void DisposeMapResultBuilder()
        {
            _mapResultBuilder?.Dispose();
            _mapResultBuilder = null;
        }

        public void Dispose()
        {
            DisposeMapResultBuilder();

            DocumentMapEntries = null;
            MapPhaseTree = null;
            ReducePhaseTree = null;
            ResultsStoreTypes = null;
            ProcessedDocEtags.Clear();
            ProcessedTombstoneEtags.Clear();
            ProcessedTimeSeriesDeletedRangeEtags.Clear();
            StoreByReduceKeyHash.Clear();
            FreedPages.Clear();

            // a document with a large fan-out does not keep its array for the life of the index
            if (MapEntries.Capacity > MaxRetainedMapEntries)
                MapEntries = [];
            else
                MapEntries.Clear();
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
            if (mapEntriesTree.TryRead(LastMapResultIdKey, out var reader) == false)
                return;

            NextMapResultId = *(long*)reader.Base;
        }
    }
}
