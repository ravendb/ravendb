using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes
{
    public sealed class IndexStorage
    {
        public sealed class Constants
        {
            private Constants()
            {
            }

            public const string DocumentReferencePrefix = "$";

            public const string DocumentReferenceTombstonePrefix = "%";

            public const string DocumentReferenceCollectionPrefix = "#";

            public const string CompareExchangeReferencePrefix = "!";

            public const string CompareExchangeReferenceTombstonePrefix = "^";

            public const string CompareExchangeReferenceCollectionPrefix = "@";
        }

        private readonly RavenLogger _logger;

        private readonly Index _index;

        internal readonly TransactionContextPool _contextPool;

        public DocumentDatabase DocumentDatabase { get; }

        private readonly TableSchema _errorsSchema = new TableSchema();

        private readonly Dictionary<string, CollectionName> _referencedCollections;

        private StorageEnvironment _environment;

        private long _lastDatabaseEtagOnIndexCreation;

        public const int MaxNumberOfKeptErrors = 500;

        internal bool SimulateCorruption = false;

        internal Exception SimulateIndexWriteException = null;

        public readonly DocumentReferences ReferencesForDocuments;

        public readonly CompareExchangeReferences ReferencesForCompareExchange;

        public long CreatedTimestampAsBinary;

        public IndexStorage(Index index, TransactionContextPool contextPool, DocumentDatabase database)
        {
            _index = index;
            _contextPool = contextPool;
            DocumentDatabase = database;
            _logger = RavenLogManager.Instance.GetLoggerForIndex<IndexStorage>(index);

            var referencedCollections = index.GetReferencedCollections();
            if (referencedCollections != null)
                _referencedCollections = referencedCollections
                    .SelectMany(x => x.Value)
                    .Distinct()
                    .ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase);

            ReferencesForDocuments = new DocumentReferences();
            ReferencesForCompareExchange = new CompareExchangeReferences();
        }

        public void Initialize(DocumentDatabase documentDatabase, StorageEnvironment environment)
        {
            _environment = environment;

            CreateSchema(documentDatabase);
        }

        private unsafe void CreateSchema(DocumentDatabase documentDatabase)
        {
            _errorsSchema.DefineIndex(new TableSchema.IndexDef
            {
                StartIndex = 0,
                // there is just a single instance of this table
                // but we need it to be local so we'll be able to compact it
                IsGlobal = false,
                Name = IndexSchema.ErrorTimestampsSlice
            });

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                _errorsSchema.Create(tx.InnerTransaction, "Errors", 16);

                var typeInt = (int)_index.Type;

                var statsTree = tx.InnerTransaction.CreateTree(IndexSchema.StatsTree);
                using (Slice.External(context.Allocator, (byte*)&typeInt, sizeof(int), out Slice tmpSlice))
                    statsTree.Add(IndexSchema.TypeSlice, tmpSlice);

                var sourceTypeInt = (int)_index.SourceType;
                using (Slice.External(context.Allocator, (byte*)&sourceTypeInt, sizeof(int), out Slice tmpSlice))
                    statsTree.Add(IndexSchema.SourceTypeSlice, tmpSlice);

                var createdTimestampResult = statsTree.Read(IndexSchema.CreatedTimestampSlice);
                if (createdTimestampResult == null)
                {
                    var binaryDate = CreatedTimestampAsBinary = SystemTime.UtcNow.ToBinary();
                    using (Slice.External(context.Allocator, (byte*)&binaryDate, sizeof(long), out Slice tmpSlice))
                        statsTree.Add(IndexSchema.CreatedTimestampSlice, tmpSlice);
                }
                else
                {
                    CreatedTimestampAsBinary = createdTimestampResult.Reader.Read<long>();
                }

                using (Slice.From(context.Allocator, documentDatabase.DbBase64Id, out var dbId))
                    statsTree.Add(IndexSchema.DatabaseIdSlice, dbId);

                tx.InnerTransaction.CreateTree(IndexSchema.EtagsTree);
                tx.InnerTransaction.CreateTree(IndexSchema.EtagsTombstoneTree);
                tx.InnerTransaction.CreateTree(IndexSchema.EtagsTimeSeriesDeletedRangeTree);
                tx.InnerTransaction.CreateTree(IndexSchema.References);
                tx.InnerTransaction.CreateTree(IndexSchema.ReferencesForCompareExchange);

                _lastDatabaseEtagOnIndexCreation = InitializeLastDatabaseEtagOnIndexCreation(context);

                _index.Definition.Persist(context, _environment.Options);

                PersistConfiguration();

                tx.Commit();

                void PersistConfiguration()
                {
                    var configurationTree = tx.InnerTransaction.CreateTree(IndexSchema.ConfigurationTree);
                    PersistSearchEngine(configurationTree);
                    AssertAndPersistAnalyzer(configurationTree, RavenConfiguration.GetKey(x => x.Indexing.DefaultAnalyzer), _index.Configuration.DefaultAnalyzer, Raven.Client.Constants.Documents.Indexing.Analyzers.Default);
                    AssertAndPersistAnalyzer(configurationTree, RavenConfiguration.GetKey(x => x.Indexing.DefaultExactAnalyzer), _index.Configuration.DefaultExactAnalyzer, Raven.Client.Constants.Documents.Indexing.Analyzers.DefaultExact);
                    AssertAndPersistAnalyzer(configurationTree, RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer), _index.Configuration.DefaultSearchAnalyzer, Raven.Client.Constants.Documents.Indexing.Analyzers.DefaultSearch);
                    PersistArchivedDataProcessingBehavior(configurationTree, _index.GetDefaultArchivedDataProcessingBehavior());
                    PersistCoraxComplexFieldIndexingBehavior(configurationTree);
                }

                void AssertAndPersistAnalyzer(Tree configurationTree, string configurationKey, string expectedAnalyzer, string defaultAnalyzer)
                {
                    var result = configurationTree.Read(configurationKey);
                    string persistedConfigurationValue = null;
                    if (result != null)
                        persistedConfigurationValue = result.Reader.ToStringValue();
                    else if (_index.Definition.Version < IndexDefinitionBaseServerSide.IndexVersion.Analyzers)
                        persistedConfigurationValue = defaultAnalyzer;

                    if (persistedConfigurationValue != null)
                    {
                        if (persistedConfigurationValue != expectedAnalyzer)
                            throw new InvalidOperationException($"Invalid analyzer. The index '{_index.Name}' was created with analyzer '{persistedConfigurationValue}' for '{configurationKey}' configuration, but current one is '{expectedAnalyzer}'. Please reset the index.");

                        return;
                    }

                    configurationTree.Add(configurationKey, expectedAnalyzer);
                }

                void PersistSearchEngine(Tree configurationTree)
                {
                    string configurationKey = nameof(SearchEngineType);
                    string configurationName = _index.Type.IsAuto() ? RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType) : RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType);

                    SearchEngineType defaultEngineType = _index.Type.IsAuto() switch
                    {
                        // We only support Vectors in Corax, so if an auto-index is using it, let's already set it up as such, regardless
                        // of what type of default storage engine is configured.
                        true when _index.Definition.IndexFields.Any(x=> x.Value.Vector != null) => SearchEngineType.Corax,
                        true => _index.Configuration.AutoIndexingEngineType,
                        false => _index.Configuration.StaticIndexingEngineType
                    };

                    if (defaultEngineType == SearchEngineType.None)
                        throw new InvalidDataException($"Default search engine is {SearchEngineType.None}. Please set {configurationName}.");
                    var result = configurationTree.Read(configurationKey);
                    if (result != null)
                    {
                        if (Enum.TryParse(result.Reader.ToStringValue(), out SearchEngineType persistedSearchEngineType) == false)
                        {
                            throw new InvalidDataException($"Invalid search engine for {_index.Name}  was saved previously or it's corrupted. Please reset the index.");
                        }
                    }
                    else
                    {
                        SearchEngineType type;

                        if (_index.Definition.Version < IndexDefinitionBaseServerSide.IndexVersion.EngineTypeStored)
                            type = SearchEngineType.Lucene;
                        else
                            type = defaultEngineType;

                        configurationTree.Add(configurationKey, type.ToString());
                    }
                }

                void PersistArchivedDataProcessingBehavior(Tree configurationTree, ArchivedDataProcessingBehavior defaultBehavior)
                {
                    const string configurationKey = nameof(ArchivedDataProcessingBehavior);

                    if (_index.Definition.ArchivedDataProcessingBehavior != null)
                        configurationTree.Add(configurationKey, _index.Definition.ArchivedDataProcessingBehavior.ToString());
                    else
                    {
                        if (configurationTree.Read(configurationKey) != null)
                            return; // do not overwrite default value if it exists already

                        configurationTree.Add(configurationKey, defaultBehavior.ToString());
                    }
                }

                void PersistCoraxComplexFieldIndexingBehavior(Tree configurationTree)
                {
                    const string configurationKey = nameof(IndexingConfiguration.CoraxComplexFieldIndexingBehavior);

                    var configuredBehavior = IndexingConfiguration.CoraxComplexFieldIndexingBehavior.None;

                    if (_index.Definition.Version >= IndexDefinitionBaseServerSide.IndexVersion.CoraxComplexFieldIndexingBehavior)
                        configuredBehavior = _index.Configuration.CoraxStaticIndexComplexFieldIndexingBehavior;

                    var result = configurationTree.Read(configurationKey);
                    if (result != null)
                    {
                        var behaviorStringValue = result.Reader.ToStringValue();

                        if (Enum.TryParse(behaviorStringValue, out IndexingConfiguration.CoraxComplexFieldIndexingBehavior _) == false)
                        {
                            throw new InvalidDataException($"Invalid indexing complex field behavior in '{_index.Name}' Corax index. Got value: '{behaviorStringValue}'");
                        }
                    }
                    else
                    {
                        configurationTree.Add(configurationKey, configuredBehavior.ToString());
                    }
                }
            }
        }

        private long InitializeLastDatabaseEtagOnIndexCreation(TransactionOperationContext indexContext)
        {
            const string key = "LastEtag";

            if (_environment.IsNew == false)
            {
                var tree = indexContext.Transaction.InnerTransaction.ReadTree(IndexSchema.LastDocumentEtagOnIndexCreationTree);
                var result = tree?.Read(key);
                return result?.Reader.Read<long>() ?? 0;
            }

            using (var queryContext = QueryOperationContext.Allocate(DocumentDatabase, _index))
            using (queryContext.OpenReadTransaction())
            using (Slice.From(indexContext.Allocator, key, out var slice))
            {
                var lastDatabaseEtag = DocumentDatabase.DocumentsStorage.ReadLastEtag(queryContext.Documents.Transaction.InnerTransaction);
                var tree = indexContext.Transaction.InnerTransaction.CreateTree(IndexSchema.LastDocumentEtagOnIndexCreationTree);
                tree.Add(slice, lastDatabaseEtag);

                return lastDatabaseEtag;
            }
        }

        public bool LowerThanLastDatabaseEtagOnIndexCreation(long currentEtag)
        {
            return _lastDatabaseEtagOnIndexCreation >= currentEtag;
        }

        public void WriteDefinition(IndexDefinitionBaseServerSide indexDefinition, TimeSpan? timeout = null)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction(timeout))
            {
                indexDefinition.Persist(context, _environment.Options);

                tx.Commit();
            }
        }

        public unsafe void WriteState(IndexState state)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);
                var stateInt = (int)state;
                using (Slice.External(context.Allocator, (byte*)&stateInt, sizeof(int), out Slice stateSlice))
                    statsTree.Add(IndexSchema.StateSlice, stateSlice);

                tx.Commit();
            }
        }

        public IndexState ReadState(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);
            var state = statsTree.Read(IndexSchema.StateSlice);
            if (state == null)
                return IndexState.Normal;

            return (IndexState)state.Reader.Read<int>();
        }

        public void DeleteErrors()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");

                table.DeleteForwardFrom(_errorsSchema.Indexes[IndexSchema.ErrorTimestampsSlice], Slices.BeforeAllKeys, startsWith: false, numberOfEntriesToDelete: long.MaxValue);

                tx.Commit();
            }
        }

        public unsafe List<IndexingError> ReadErrors()
        {
            var errors = new List<IndexingError>();

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");

                foreach (var tvr in table.SeekForwardFrom(_errorsSchema.Indexes[IndexSchema.ErrorTimestampsSlice], Slices.BeforeAllKeys, 0))
                {
                    var error = new IndexingError();

                    var ptr = tvr.Result.Reader.Read(0, out int size);
                    error.Timestamp = new DateTime(Bits.SwapBytes(*(long*)ptr), DateTimeKind.Utc);

                    ptr = tvr.Result.Reader.Read(1, out size);
                    if (size != 0)
                        error.Document = context.AllocateStringValue(null, ptr, size);

                    ptr = tvr.Result.Reader.Read(2, out size);
                    error.Action = context.AllocateStringValue(null, ptr, size);

                    ptr = tvr.Result.Reader.Read(3, out size);
                    error.Error = context.AllocateStringValue(null, ptr, size);

                    errors.Add(error);
                }
            }

            return errors;
        }

        public long ReadErrorsCount()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");
                return table.NumberOfEntries;
            }
        }

        public unsafe DateTime? ReadLastIndexingErrorTime()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");

                using (var it = table.GetTree(_errorsSchema.Indexes[IndexSchema.ErrorTimestampsSlice]).Iterate(false))
                {
                    if (it.Seek(Slices.AfterAllKeys) == false)
                        return null;

                    var ptr = it.CurrentKey.Content.Ptr;

                    return new DateTime(Bits.SwapBytes(*(long*)ptr), DateTimeKind.Utc);
                }
            }
        }

        public DateTime? ReadLastIndexingTime(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);

            var lastIndexingTime = statsTree.Read(IndexSchema.LastIndexingTimeSlice);
            if (lastIndexingTime == null)
                return null;

            return DateTime.FromBinary(lastIndexingTime.Reader.Read<long>());
        }

        public bool IsIndexInvalid(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);

            var mapAttempts = statsTree.Read(IndexSchema.MapAttemptsSlice)?.Reader.Read<long>() ?? 0;
            var mapErrors = statsTree.Read(IndexSchema.MapErrorsSlice)?.Reader.Read<long>() ?? 0;

            long? reduceAttempts = null, reduceErrors = null;

            if (_index.Type.IsMapReduce())
            {
                reduceAttempts = statsTree.Read(IndexSchema.ReduceAttemptsSlice)?.Reader.Read<long>() ?? 0;
                reduceErrors = statsTree.Read(IndexSchema.ReduceErrorsSlice)?.Reader.Read<long>() ?? 0;
            }

            long mapReferenceAttempts = 0, mapReferenceErrors = 0;
            if (_index.GetReferencedCollections()?.Count > 0)
            {
                mapReferenceAttempts = statsTree.Read(IndexSchema.MapReferencedAttemptsSlice)?.Reader.Read<long>() ?? 0;
                mapReferenceErrors = statsTree.Read(IndexSchema.MapReferenceErrorsSlice)?.Reader.Read<long>() ?? 0;
            }

            return IndexFailureInformation.CheckIndexInvalid(mapAttempts, mapErrors,
                mapReferenceAttempts, mapReferenceErrors, reduceAttempts, reduceErrors, false);
        }

        public IndexStats ReadStats(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);
            var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");

            var stats = new IndexStats
            {
                CreatedTimestamp = DateTime.FromBinary(CreatedTimestampAsBinary),
                ErrorsCount = (int)(table?.NumberOfEntries ?? 0)
            };

            var lastIndexingTime = statsTree.Read(IndexSchema.LastIndexingTimeSlice);

            stats.Collections = new Dictionary<string, IndexStats.CollectionStats>();
            foreach (var collection in _index.Definition.Collections)
            {
                stats.Collections[collection] = new IndexStats.CollectionStats
                {
                    LastProcessedDocumentEtag = ReadLastIndexedEtag(tx, collection),
                    LastProcessedTombstoneEtag = ReadLastProcessedTombstoneEtag(tx, collection)
                };
            }

            var entriesCountReader = statsTree.Read(IndexSchema.EntriesCount)?.Reader;
            long? entriesCount = null;
            if (entriesCountReader.HasValue)
            {
                var entriesCountSize = entriesCountReader.Value.Length;
                //backward compatibility https://github.com/ravendb/ravendb/commit/5c53b01ee2b4fad8f3ef410f3e4976144d72c023
                entriesCount = entriesCountSize == sizeof(long) 
                    ? entriesCountReader.Value.Read<long>() 
                    : entriesCountReader.Value.Read<int>();
            }

            if (entriesCount != null)
                stats.EntriesCount = entriesCount.Value;
            else
            {
                using (var reader = _index.IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                {
                    stats.EntriesCount = reader.EntriesCount();
                }
            }

            if (lastIndexingTime != null)
            {
                stats.LastIndexingTime = DateTime.FromBinary(lastIndexingTime.Reader.Read<long>());
                stats.MapAttempts = statsTree.Read(IndexSchema.MapAttemptsSlice).Reader.Read<int>();
                stats.MapErrors = statsTree.Read(IndexSchema.MapErrorsSlice).Reader.Read<int>();
                stats.MapSuccesses = statsTree.Read(IndexSchema.MapSuccessesSlice).Reader.Read<int>();
                stats.MaxNumberOfOutputsPerDocument =
                    statsTree.Read(IndexSchema.MaxNumberOfOutputsPerDocument).Reader.Read<int>();

                if (_index.Type.IsMapReduce())
                {
                    stats.ReduceAttempts = statsTree.Read(IndexSchema.ReduceAttemptsSlice)?.Reader.Read<long>() ?? 0;
                    stats.ReduceSuccesses = statsTree.Read(IndexSchema.ReduceSuccessesSlice)?.Reader.Read<long>() ?? 0;
                    stats.ReduceErrors = statsTree.Read(IndexSchema.ReduceErrorsSlice)?.Reader.Read<long>() ?? 0;
                }

                if (_index.GetReferencedCollections()?.Count > 0)
                {
                    stats.MapReferenceAttempts = statsTree.Read(IndexSchema.MapReferencedAttemptsSlice)?.Reader.Read<int>() ?? 0;
                    stats.MapReferenceSuccesses = statsTree.Read(IndexSchema.MapReferenceSuccessesSlice)?.Reader.Read<int>() ?? 0;
                    stats.MapReferenceErrors = statsTree.Read(IndexSchema.MapReferenceErrorsSlice)?.Reader.Read<int>() ?? 0;
                }
            }

            return stats;
        }

        public int ReadMaxNumberOfOutputsPerDocument(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);

            var lastIndexingTime = statsTree.Read(IndexSchema.LastIndexingTimeSlice);

            if (lastIndexingTime != null)
            {
                return statsTree.Read(IndexSchema.MaxNumberOfOutputsPerDocument).Reader.Read<int>();
            }

            return 0;
        }

        public ArchivedDataProcessingBehavior ReadArchivedDataProcessingBehavior(RavenTransaction tx)
        {
            var configurationTree = tx.InnerTransaction.ReadTree(IndexSchema.ConfigurationTree);
            if (configurationTree == null)
            {
                throw new InvalidOperationException($"Index does not contain {nameof(IndexSchema.ConfigurationTree)}' tree.");
            }

            var result = configurationTree.Read(IndexSchema.ArchivedDataProcessingBehaviorSlice);
            if (result == null)
            {
                throw new InvalidOperationException($"Index does not contain {nameof(IndexSchema.ArchivedDataProcessingBehaviorSlice)}' tree.");
            }

            if (Enum.TryParse(result.Reader.ToStringValue(), out ArchivedDataProcessingBehavior persistedArchivedDataProcessingBehavior) == false)
            {
                throw new InvalidOperationException($"Index does not contain valid {nameof(ArchivedDataProcessingBehavior)} property. It contains: {result.Reader.ToStringValue()}.");
            }

            return persistedArchivedDataProcessingBehavior;
        }

        public IndexingConfiguration.CoraxComplexFieldIndexingBehavior ReadCoraxComplexFieldIndexingBehavior(RavenTransaction tx)
        {
            var configurationTree = tx.InnerTransaction.ReadTree(IndexSchema.ConfigurationTree);
            if (configurationTree == null)
            {
                throw new InvalidOperationException($"Index does not contain {nameof(IndexSchema.ConfigurationTree)}' tree.");
            }

            var result = configurationTree.Read(IndexSchema.CoraxComplexFieldIndexingBehavior);
            if (result == null)
            {
                throw new InvalidOperationException($"Index does not contain {nameof(IndexSchema.CoraxComplexFieldIndexingBehavior)}' key.");
            }

            if (Enum.TryParse(result.Reader.ToStringValue(), out IndexingConfiguration.CoraxComplexFieldIndexingBehavior persistedCoraxStaticIndexComplexFieldIndexingBehavior) == false)
            {
                throw new InvalidDataException($"Invalid indexing complex field behavior in '{_index.Name}' Corax index. It has: {result.Reader.ToStringValue()} defined.");
            }

            return persistedCoraxStaticIndexComplexFieldIndexingBehavior;
        }

        public sealed class DocumentReferences : ReferencesBase
        {
            public DocumentReferences()
                : base(IndexSchema.References, Constants.DocumentReferencePrefix, Constants.DocumentReferenceTombstonePrefix, Constants.DocumentReferenceCollectionPrefix, ReferencesType.Documents)
            {
            }
        }

        public sealed class CompareExchangeReferences : ReferencesBase
        {
            public static CollectionName CompareExchange = new CollectionName(nameof(CompareExchange));

            public CompareExchangeReferences()
                : base(IndexSchema.ReferencesForCompareExchange, Constants.CompareExchangeReferencePrefix, Constants.CompareExchangeReferenceTombstonePrefix, Constants.CompareExchangeReferenceCollectionPrefix, ReferencesType.CompareExchange)
            {
            }
        }

        public abstract class ReferencesBase
        {
            private readonly string _referenceTreeName;
            private readonly string _referencePrefix;
            private readonly string _referenceTombstonePrefix;
            private readonly string _referenceCollectionPrefix;
            private readonly ReferencesType _type;

            public enum ReferencesType
            {
                Documents,
                CompareExchange
            }

            protected ReferencesBase(string referencesTreeName, string referencePrefix, string referenceTombstonePrefix, string referenceCollectionPrefix, ReferencesType type)
            {
                _referenceTreeName = referencesTreeName ?? throw new ArgumentNullException(nameof(referencesTreeName));
                _referencePrefix = referencePrefix ?? throw new ArgumentNullException(nameof(referencePrefix));
                _referenceTombstonePrefix = referenceTombstonePrefix ?? throw new ArgumentNullException(nameof(referenceTombstonePrefix));
                _referenceCollectionPrefix = referenceCollectionPrefix ?? throw new ArgumentNullException(nameof(referenceCollectionPrefix));
                _type = type;
            }

            public long ReadLastProcessedReferenceEtag(Transaction tx, string collection, CollectionName referencedCollection)
            {
                if (tx.IsWriteTransaction == false && tx.LowLevelTransaction.TryGetClientState(out IndexStateRecord cache))
                {
                    switch (_type)
                    {
                        case ReferencesType.Documents:
                            if (cache.Collections.TryGetValue(collection, out var val) &&
                                val.LastReferencedEtags?.TryGetValue(referencedCollection.Name, out var documentEtags) == true)
                            {
                                return documentEtags.LastEtag;
                            }
                            break;

                        case ReferencesType.CompareExchange:
                            if (cache.Collections.TryGetValue(collection, out var compareExchangeEtags) &&
                                compareExchangeEtags.LastReferencedEtagsForCompareExchange != null)
                            {
                                return compareExchangeEtags.LastReferencedEtagsForCompareExchange.LastEtag;
                            }
                            break;
                    }
                }

                var tree = tx.ReadTree(_referencePrefix + collection);

                var result = tree?.Read(referencedCollection.Name);
                if (result == null)
                    return 0;

                return result.Reader.Read<long>();
            }

            public unsafe void WriteLastReferenceEtag(RavenTransaction tx, string collection, CollectionName referencedCollection, long etag)
            {
                var tree = tx.InnerTransaction.CreateTree(_referencePrefix + collection);
                using (Slice.From(tx.InnerTransaction.Allocator, referencedCollection.Name, ByteStringType.Immutable, out Slice collectionSlice))
                using (Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long), out Slice etagSlice))
                {
                    tree.Add(collectionSlice, etagSlice);
                }
            }

            public long ReadLastProcessedReferenceTombstoneEtag(Transaction tx, string collection, CollectionName referencedCollection)
            {
                if (tx.IsWriteTransaction == false && tx.LowLevelTransaction.TryGetClientState(out IndexStateRecord cache))
                {
                    switch (_type)
                    {
                        case ReferencesType.Documents:
                            if (cache.Collections.TryGetValue(collection, out var val) &&
                                val.LastReferencedEtags?.TryGetValue(referencedCollection.Name, out var documentEtags) == true)
                            {
                                return documentEtags.LastProcessedTombstoneEtag;
                            }
                            break;

                        case ReferencesType.CompareExchange:
                            if (cache.Collections.TryGetValue(collection, out var compareExchangeEtags) &&
                                compareExchangeEtags.LastReferencedEtagsForCompareExchange != null)
                            {
                                return compareExchangeEtags.LastReferencedEtagsForCompareExchange.LastProcessedTombstoneEtag;
                            }
                            break;
                    }
                }

                var tree = tx.ReadTree(_referenceTombstonePrefix + collection);

                var result = tree?.Read(referencedCollection.Name);
                if (result == null)
                    return 0;

                return result.Reader.Read<long>();
            }

            public unsafe void WriteLastReferenceTombstoneEtag(RavenTransaction tx, string collection, CollectionName referencedCollection, long etag)
            {
                var tree = tx.InnerTransaction.CreateTree(_referenceTombstonePrefix + collection);
                using (Slice.From(tx.InnerTransaction.Allocator, referencedCollection.Name, ByteStringType.Immutable, out Slice collectionSlice))
                using (Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long), out Slice etagSlice))
                {
                    tree.Add(collectionSlice, etagSlice);
                }
            }

            public IEnumerable<Slice> GetItemKeysFromCollectionThatReference(string collection, LazyStringValue referenceKey, RavenTransaction tx, string lastItemId = null)
            {
                var collectionTree = tx.InnerTransaction.ReadTree(_referenceCollectionPrefix + collection);
                if (collectionTree == null)
                    yield break;

                using (DocumentIdWorker.GetLower(tx.InnerTransaction.Allocator, referenceKey, out var k))
                using (var it = collectionTree.MultiRead(k))
                {
                    if (lastItemId == null)
                    {
                        if (it.Seek(Slices.BeforeAllKeys) == false)
                            yield break;
                    }
                    else
                    {
                        using (Slice.From(tx.InnerTransaction.Allocator, lastItemId, out var idSlice))
                        {
                            if (it.Seek(idSlice) == false)
                                yield break;
                        }
                    }

                    do
                    {
                        yield return it.CurrentKey;
                    } while (it.MoveNext());
                }
            }

            public void RemoveReferences(Slice key, string collection, HashSet<Slice> referenceKeysToSkip, RavenTransaction tx)
            {
                var referencesTree = tx.InnerTransaction.ReadTree(_referenceTreeName);

                List<Slice> referenceKeys;
                using (var it = referencesTree.MultiRead(key))
                {
                    if (it.Seek(Slices.BeforeAllKeys) == false)
                        return;

                    referenceKeys = new List<Slice>();

                    do
                    {
                        if (referenceKeysToSkip == null || referenceKeysToSkip.Contains(it.CurrentKey) == false)
                            referenceKeys.Add(it.CurrentKey.Clone(tx.InnerTransaction.Allocator, ByteStringType.Immutable));
                    } while (it.MoveNext());
                }

                if (referenceKeys.Count == 0)
                    return;

                var collectionTree = tx.InnerTransaction.ReadTree(_referenceCollectionPrefix + collection);

                foreach (var referenceKey in referenceKeys)
                {
                    referencesTree.MultiDelete(key, referenceKey);
                    collectionTree?.MultiDelete(referenceKey, key);
                    referenceKey.Release(tx.InnerTransaction.Allocator);
                }
            }

            public void RemoveReferencesByPrefix(Slice prefixKey, string collection, HashSet<Slice> referenceKeysToSkip, RavenTransaction tx)
            {
                var referencesTree = tx.InnerTransaction.ReadTree(_referenceTreeName);

                while (true)
                {
                    using (var it = referencesTree.Iterate(false))
                    {
                        it.SetRequiredPrefix(prefixKey);

                        if (it.Seek(prefixKey) == false)
                            return;

                        var key = it.CurrentKey.Clone(tx.InnerTransaction.Allocator);

                        try
                        {
                            RemoveReferences(key, collection, referenceKeysToSkip, tx);
                        }
                        finally
                        {
                            key.Release(tx.InnerTransaction.Allocator);
                        }
                    }
                }
            }

            public void WriteReferences(Dictionary<string, Dictionary<Slice, HashSet<Slice>>> referencesByCollection, RavenTransaction tx)
            {
                var referencesTree = tx.InnerTransaction.ReadTree(_referenceTreeName);

                foreach (var collections in referencesByCollection)
                {
                    WriteReferencesForSingleCollectionInternal(referencesTree, collections.Key, collections.Value, tx);
                }
            }

            private void WriteReferencesForSingleCollectionInternal(Tree referencesTree, string collection, Dictionary<Slice, HashSet<Slice>> references, RavenTransaction tx)
            {
                var collectionTree = tx.InnerTransaction.CreateTree(_referenceCollectionPrefix + collection); // #collection

                foreach (var keys in references)
                {
                    var key = keys.Key;
                    foreach (var referenceKey in keys.Value)
                    {
                        collectionTree.MultiAdd(referenceKey, key);
                        referencesTree.MultiAdd(key, referenceKey);
                    }

                    RemoveReferences(key, collection, keys.Value, tx);
                }
            }

            public void WriteReferencesForSingleCollection(string collection, Dictionary<Slice, HashSet<Slice>> references, RavenTransaction tx)
            {
                var referencesTree = tx.InnerTransaction.ReadTree(_referenceTreeName);
                WriteReferencesForSingleCollectionInternal(referencesTree, collection, references, tx);
            }

            internal (long ReferenceTableCount, long CollectionTableCount) GetReferenceTablesCount(string collection, RavenTransaction tx)
            {
                var referencesTree = tx.InnerTransaction.ReadTree(_referenceTreeName);

                var referencesCount = referencesTree.ReadHeader().NumberOfEntries;

                var collectionTree = tx.InnerTransaction.ReadTree(_referenceCollectionPrefix + collection);

                if (collectionTree != null)
                    return (referencesCount, collectionTree.ReadHeader().NumberOfEntries);

                return (referencesCount, 0);
            }
        }

        public long ReadLastProcessedTombstoneEtag(RavenTransaction tx, string collection)
        {
            var txi = tx.InnerTransaction;
            if (txi.IsWriteTransaction == false && txi.LowLevelTransaction.TryGetClientState(out IndexStateRecord cache))
            {
                if (cache.Collections.TryGetValue(collection, out var val))
                    return val.LastProcessedTombstoneEtag;
            }

            using (Slice.From(txi.Allocator, collection, out Slice collectionSlice))
            {
                return ReadLastEtag(txi, IndexSchema.EtagsTombstoneTree, collectionSlice);
            }
        }

        public long ReadLastProcessedTimeSeriesDeletedRangeEtag(RavenTransaction tx, string collection)
        {
            var txi = tx.InnerTransaction;

            if (txi.IsWriteTransaction == false && txi.LowLevelTransaction.TryGetClientState(out IndexStateRecord cache))
            {
                if (cache.Collections.TryGetValue(collection, out var val))
                    return val.LastProcessedTimeSeriesDeletedRangeEtag;
            }

            using (Slice.From(txi.Allocator, collection, out Slice collectionSlice))
            {
                return ReadLastEtag(txi, IndexSchema.EtagsTimeSeriesDeletedRangeTree, collectionSlice);
            }
        }

        public long ReadLastIndexedEtag(RavenTransaction tx, string collection)
        {
            var txi = tx.InnerTransaction;
            if (txi.IsWriteTransaction == false && txi.LowLevelTransaction.TryGetClientState(out IndexStateRecord cache))
            {
                if (cache.Collections.TryGetValue(collection, out var val))
                    return val.LastIndexedEtag;
            }

            using (Slice.From(txi.Allocator, collection, out Slice collectionSlice))
            {
                return ReadLastEtag(txi, IndexSchema.EtagsTree, collectionSlice);
            }
        }

        public void WriteLastTombstoneEtag(RavenTransaction tx, string collection, long etag)
        {
            using (Slice.From(tx.InnerTransaction.Allocator, collection, out Slice collectionSlice))
            {
                WriteLastEtag(tx, IndexSchema.EtagsTombstoneTree, collectionSlice, etag);
            }
        }

        public void WriteLastTimeSeriesDeletedRangeEtag(RavenTransaction tx, string collection, long etag)
        {
            using (Slice.From(tx.InnerTransaction.Allocator, collection, out Slice collectionSlice))
            {
                WriteLastEtag(tx, IndexSchema.EtagsTimeSeriesDeletedRangeTree, collectionSlice, etag);
            }
        }

        public void WriteLastIndexedEtag(RavenTransaction tx, string collection, long etag)
        {
            using (Slice.From(tx.InnerTransaction.Allocator, collection, out Slice collectionSlice))
            {
                WriteLastEtag(tx, IndexSchema.EtagsTree, collectionSlice, etag);
            }
        }

        private unsafe void WriteLastEtag(RavenTransaction tx, string tree, Slice collection, long etag)
        {
            if (SimulateCorruption)
                SimulateCorruptionError();

            if (SimulateIndexWriteException != null)
                SimulateIndexWriteError(SimulateIndexWriteException);

            if (_logger.IsDebugEnabled)
                _logger.Debug($"Writing last etag for '{_index.Name}'. Tree: {tree}. Collection: {collection}. Etag: {etag}.");

            var statsTree = tx.InnerTransaction.CreateTree(tree);
            using (Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long), out Slice etagSlice))
                statsTree.Add(collection, etagSlice);
        }

        private void SimulateCorruptionError()
        {
            try
            {
                throw new SimulatedVoronUnrecoverableErrorException("Simulated corruption.");
            }
            catch (Exception e)
            {
                _environment.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
        }

        private void SimulateIndexWriteError(Exception inner)
        {
            throw new IndexWriteException(inner);
        }

        public sealed class SimulatedVoronUnrecoverableErrorException : VoronUnrecoverableErrorException
        {
            public SimulatedVoronUnrecoverableErrorException(string message) : base(message)
            {
            }
        }

        internal static long ReadLastEtag(Transaction tx, string tree, Slice collection)
        {
            var statsTree = tx.CreateTree(tree);
            
            long lastEtag = 0;
            if (statsTree.TryRead(collection, out var reader))
                lastEtag = reader.Read<long>();

            return lastEtag;
        }

        public unsafe IndexFailureInformation UpdateStats(DateTime indexingTime, IndexingRunStats stats)
        {
            if (_logger.IsDebugEnabled)
                _logger.Debug($"Updating statistics for '{_index.Name}'. Stats: {stats}.");

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var result = new IndexFailureInformation
                {
                    Name = _index.Name
                };

                var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");

                var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);

                result.MapAttempts = statsTree.Increment(IndexSchema.MapAttemptsSlice, stats.MapAttempts);
                result.MapSuccesses = statsTree.Increment(IndexSchema.MapSuccessesSlice, stats.MapSuccesses);
                result.MapErrors = statsTree.Increment(IndexSchema.MapErrorsSlice, stats.MapErrors);

                var currentMaxNumberOfOutputs = statsTree.Read(IndexSchema.MaxNumberOfOutputsPerDocument)?.Reader.Read<int>();

                using (statsTree.DirectAdd(IndexSchema.MaxNumberOfOutputsPerDocument, sizeof(int), out byte* ptr))
                {
                    *(int*)ptr = currentMaxNumberOfOutputs > stats.MaxNumberOfOutputsPerDocument
                        ? currentMaxNumberOfOutputs.Value
                        : stats.MaxNumberOfOutputsPerDocument;
                }

                if (_index.Type.IsMapReduce())
                {
                    result.ReduceAttempts = statsTree.Increment(IndexSchema.ReduceAttemptsSlice, stats.ReduceAttempts);
                    result.ReduceSuccesses = statsTree.Increment(IndexSchema.ReduceSuccessesSlice, stats.ReduceSuccesses);
                    result.ReduceErrors = statsTree.Increment(IndexSchema.ReduceErrorsSlice, stats.ReduceErrors);
                }

                if (_index.GetReferencedCollections()?.Count > 0)
                {
                    result.MapReferenceAttempts = statsTree.Increment(IndexSchema.MapReferencedAttemptsSlice, stats.MapReferenceAttempts);
                    result.MapReferenceSuccesses = statsTree.Increment(IndexSchema.MapReferenceSuccessesSlice, stats.MapReferenceSuccesses);
                    result.MapReferenceErrors = statsTree.Increment(IndexSchema.MapReferenceErrorsSlice, stats.MapReferenceErrors);
                }

                if (stats.EntriesCount != null) // available only when tx was committed
                    statsTree.Add(IndexSchema.EntriesCount, stats.EntriesCount.Value);

                var binaryDate = indexingTime.ToBinary();
                using (Slice.External(context.Allocator, (byte*)&binaryDate, sizeof(long), out Slice binaryDateslice))
                    statsTree.Add(IndexSchema.LastIndexingTimeSlice, binaryDateslice);

                if (stats.Errors != null)
                {
                    for (var i = Math.Max(stats.Errors.Count - MaxNumberOfKeptErrors, 0); i < stats.Errors.Count; i++)
                    {
                        var error = stats.Errors[i];
                        var ticksBigEndian = Bits.SwapBytes(error.Timestamp.Ticks);
                        using (var document = context.GetLazyString(error.Document))
                        using (var action = context.GetLazyString(error.Action))
                        using (var e = context.GetLazyString(error.Error))
                        {
                            var tvb = new TableValueBuilder
                            {
                                {(byte*) &ticksBigEndian, sizeof (long)},
                                {document.Buffer, document.Size},
                                {action.Buffer, action.Size},
                                {e.Buffer, e.Size}
                            };
                            table.Insert(tvb);
                        }
                    }

                    CleanupErrors(table);
                }

                tx.Commit();

                return result;
            }
        }

        private void CleanupErrors(Table table)
        {
            if (table.NumberOfEntries <= MaxNumberOfKeptErrors)
                return;

            var numberOfEntriesToDelete = table.NumberOfEntries - MaxNumberOfKeptErrors;
            table.DeleteForwardFrom(_errorsSchema.Indexes[IndexSchema.ErrorTimestampsSlice], Slices.BeforeAllKeys, false, numberOfEntriesToDelete);
        }

        public static IndexType ReadIndexType(string name, StorageEnvironment environment)
        {
            using (var tx = environment.ReadTransaction())
            {
                var statsTree = tx.ReadTree(IndexSchema.StatsTree);
                if (statsTree == null)
                    throw new InvalidOperationException($"Index '{name}' does not contain 'Stats' tree.");

                if (statsTree.TryRead(IndexSchema.TypeSlice, out var reader) == false)
                    throw new InvalidOperationException($"Stats tree does not contain 'Type' entry in index '{name}'.");

                return (IndexType)reader.Read<int>();
            }
        }

        public static string ReadDatabaseId(string name, StorageEnvironment environment)
        {
            using (var tx = environment.ReadTransaction())
            {
                var statsTree = tx.ReadTree(IndexSchema.StatsTree);
                if (statsTree == null)
                    throw new InvalidOperationException($"Index '{name}' does not contain 'Stats' tree.");

                if (statsTree.TryRead(IndexSchema.DatabaseIdSlice, out var reader) == false)
                    return null; // backward compatibility

                return reader.ReadString(reader.Length);
            }
        }

        public static SearchEngineType ReadSearchEngineType(string name, StorageEnvironment environment)
        {
            using (var tx = environment.ReadTransaction())
            {
                var configurationTree = tx.ReadTree(IndexSchema.ConfigurationTree);
                if (configurationTree == null)
                {
                    throw new InvalidOperationException($"Index '{name}' does not contain {nameof(IndexSchema.ConfigurationTree)}' tree.");
                }

                var result = configurationTree.Read(IndexSchema.SearchEngineType);
                if (result == null)
                {
                    return SearchEngineType.None;
                }

                if (Enum.TryParse(result.Reader.ToStringValue(), out SearchEngineType persistedSearchEngineType) == false)
                {
                    throw new InvalidOperationException($"Index '{name}' does not contain valid {nameof(SearchEngineType)} property. It contains: {result.Reader.ToStringValue()}.");
                }

                return persistedSearchEngineType;
            }
        }

        public static IndexSourceType ReadIndexSourceType(string name, StorageEnvironment environment)
        {
            using (var tx = environment.ReadTransaction())
            {
                var statsTree = tx.ReadTree(IndexSchema.StatsTree);
                if (statsTree == null)
                    throw new InvalidOperationException($"Index '{name}' does not contain 'Stats' tree.");

                if (statsTree.TryRead(IndexSchema.SourceTypeSlice, out var reader) == false)
                    return IndexSourceType.Documents; // backward compatibility

                return (IndexSourceType)reader.Read<int>();
            }
        }

        public unsafe void WriteReferences(CurrentIndexingScope indexingScope, RavenTransaction tx)
        {
            // IndexSchema:
            // having 'Users' and 'Addresses' we will end up with
            //
            // #Users (tree) - splitted by collection so we can easily return all items of same collection to the indexing function
            // |- addresses/1 (key) -> [ users/1, users/2 ]
            // |- addresses/2 (key) -> [ users/3 ]
            //
            // References (tree) - used in delete operations
            // |- users/1 -> [ addresses/1 ]
            // |- users/2 -> [ addresses/1 ]
            // |- users/3 -> [ addresses/2 ]
            //
            // $Users (tree) - holding highest visible etag of 'referenced collection' per collection, so we will have a starting point for references processing
            // |- Addresses (key) -> 5
            if (indexingScope.ReferencesByCollection != null)
                ReferencesForDocuments.WriteReferences(indexingScope.ReferencesByCollection, tx);

            if (indexingScope.ReferencesByCollectionForCompareExchange != null)
                ReferencesForCompareExchange.WriteReferences(indexingScope.ReferencesByCollectionForCompareExchange, tx);
        }

        public void Rename(string name)
        {
            if (_index.Definition.Name == name)
                return;

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                _index.Definition.Rename(name, context, _environment.Options);

                tx.Commit();
            }
        }

        internal HashSet<string> ReadIndexTimeFields()
        {
            HashSet<string> fields;
            var searchEngineType = ReadSearchEngineType(_index.Name, _environment);
            using (var tx = _environment.ReadTransaction())
            {
                fields = searchEngineType == SearchEngineType.Corax
                    ? Corax.Utils.TimeFields.ReadTimeFieldsNames(tx)
                    : ReadLuceneTimeFields(tx);
            }

            return fields;

            HashSet<string> ReadLuceneTimeFields(Transaction tx)
            {
                HashSet<string> container = new();
                var fieldsTree = tx.ReadTree(IndexSchema.FieldsTree);
                if (fieldsTree != null)
                {
                    using (var it = fieldsTree.MultiRead(IndexSchema.TimeSlice))
                    {
                        if (it.Seek(Slices.BeforeAllKeys))
                        {
                            do
                            {
                                container.Add(it.CurrentKey.ToString());
                            } while (it.MoveNext());
                        }
                    }
                }

                return container;
            }
        }

        internal void WriteIndexTimeFields(RavenTransaction tx, HashSet<string> timeFieldsToAdd)
        {
            if (_index.SearchEngineType == SearchEngineType.Corax)
            {
                Corax.Utils.TimeFields.WriteTimeFieldsNames(tx.InnerTransaction, timeFieldsToAdd);
            }
            else
            {
                var fieldsTree = tx.InnerTransaction.CreateTree(IndexSchema.FieldsTree);

                foreach (var fieldName in timeFieldsToAdd)
                    fieldsTree.MultiAdd(IndexSchema.TimeSlice, fieldName);
            }
        }

        internal sealed class IndexSchema
        {
            public const string ConfigurationTree = "Configuration";

            public const string StatsTree = "Stats";

            public const string EtagsTree = "Etags";

            public const string FieldsTree = "Fields";

            public const string EtagsTombstoneTree = "Etags.Tombstone";

            public const string EtagsTimeSeriesDeletedRangeTree = "Etags.TimeSeriesDeletedRange";

            public const string References = "References";

            public const string ReferencesForCompareExchange = "ReferencesForCompareExchange";

            public const string LastDocumentEtagOnIndexCreationTree = "LastDocumentEtagOnIndexCreation";

            public static readonly Slice TypeSlice;

            public static readonly Slice DatabaseIdSlice;

            public static readonly Slice SourceTypeSlice;

            public static readonly Slice ArchivedDataProcessingBehaviorSlice;

            public static readonly Slice CreatedTimestampSlice;

            public static readonly Slice MapAttemptsSlice;

            public static readonly Slice MapSuccessesSlice;

            public static readonly Slice MapErrorsSlice;

            public static readonly Slice MapReferencedAttemptsSlice;

            public static readonly Slice MapReferenceSuccessesSlice;

            public static readonly Slice MapReferenceErrorsSlice;

            public static readonly Slice ReduceAttemptsSlice;

            public static readonly Slice ReduceSuccessesSlice;

            public static readonly Slice ReduceErrorsSlice;

            public static readonly Slice LastIndexingTimeSlice;

            public static readonly Slice StateSlice;

            public static readonly Slice ErrorTimestampsSlice;

            public static readonly Slice MaxNumberOfOutputsPerDocument;

            public static readonly Slice EntriesCount;

            public static readonly Slice TimeSlice;

            public static readonly Slice SearchEngineType;

            public static readonly Slice CoraxComplexFieldIndexingBehavior;

            static IndexSchema()
            {
                using (StorageEnvironment.GetStaticContext(out var ctx))
                {
                    Slice.From(ctx, "Type", ByteStringType.Immutable, out TypeSlice);
                    Slice.From(ctx, "DatabaseId", ByteStringType.Immutable, out DatabaseIdSlice);
                    Slice.From(ctx, "SourceType", ByteStringType.Immutable, out SourceTypeSlice);
                    Slice.From(ctx, "CreatedTimestamp", ByteStringType.Immutable, out CreatedTimestampSlice);
                    Slice.From(ctx, "MapAttempts", ByteStringType.Immutable, out MapAttemptsSlice);
                    Slice.From(ctx, "MapReferencedAttempts", ByteStringType.Immutable, out MapReferencedAttemptsSlice);
                    Slice.From(ctx, "MapSuccesses", ByteStringType.Immutable, out MapSuccessesSlice);
                    Slice.From(ctx, "MapReferenceSuccesses", ByteStringType.Immutable, out MapReferenceSuccessesSlice);
                    Slice.From(ctx, "MapErrors", ByteStringType.Immutable, out MapErrorsSlice);
                    Slice.From(ctx, "MapReferenceErrors", ByteStringType.Immutable, out MapReferenceErrorsSlice);
                    Slice.From(ctx, "ReduceAttempts", ByteStringType.Immutable, out ReduceAttemptsSlice);
                    Slice.From(ctx, "ReduceSuccesses", ByteStringType.Immutable, out ReduceSuccessesSlice);
                    Slice.From(ctx, "ReduceErrors", ByteStringType.Immutable, out ReduceErrorsSlice);
                    Slice.From(ctx, "LastIndexingTime", ByteStringType.Immutable, out LastIndexingTimeSlice);
                    Slice.From(ctx, "Priority", ByteStringType.Immutable, out _);
                    Slice.From(ctx, "State", ByteStringType.Immutable, out StateSlice);
                    Slice.From(ctx, "ErrorTimestamps", ByteStringType.Immutable, out ErrorTimestampsSlice);
                    Slice.From(ctx, "MaxNumberOfOutputsPerDocument", ByteStringType.Immutable, out MaxNumberOfOutputsPerDocument);
                    Slice.From(ctx, "EntriesCount", ByteStringType.Immutable, out EntriesCount);
                    Slice.From(ctx, "Time", ByteStringType.Immutable, out TimeSlice);
                    Slice.From(ctx, nameof(Client.Documents.Indexes.SearchEngineType), ByteStringType.Immutable, out SearchEngineType);
                    Slice.From(ctx, nameof(ArchivedDataProcessingBehavior), ByteStringType.Immutable, out ArchivedDataProcessingBehaviorSlice);
                    Slice.From(ctx, nameof(CoraxComplexFieldIndexingBehavior), ByteStringType.Immutable, out CoraxComplexFieldIndexingBehavior);
                }
            }
        }

        public StorageEnvironment Environment()
        {
            return _environment;
        }
    }
}
