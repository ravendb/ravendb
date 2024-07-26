using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Queries;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Exceptions;
using Sparrow.Threading;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;

using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public sealed class LuceneIndexPersistence : IndexPersistenceBase
    {
        private readonly Analyzer _dummyAnalyzer = new SimpleAnalyzer();
        internal IndexReader _lastReader;
        private readonly LuceneDocumentConverterBase _converter;
        private static readonly StopAnalyzer StopAnalyzer = new StopAnalyzer(Version.LUCENE_30);

        private LuceneIndexWriter _indexWriter;
        public override bool HasWriter => _indexWriter != null;

        private Dictionary<string, LuceneSuggestionIndexWriter> _suggestionsIndexWriters;

        private SnapshotDeletionPolicy _snapshotter;

        internal TempFileCache TempFileCache;


        private LuceneVoronDirectory _directory;
        public LuceneVoronDirectory LuceneDirectory { get { return _directory; } }

        private readonly Dictionary<string, LuceneVoronDirectory> _suggestionsDirectories;

        private readonly DisposeOnce<SingleAttempt> _disposeOnce;

        private bool _initialized;
        private readonly Dictionary<string, IndexField> _fields;
        private readonly Logger _logger;

        private readonly object _readersLock = new object();

        public LuceneIndexPersistence(Index index, IIndexReadOperationFactory indexReadOperationFactory) : base(index, indexReadOperationFactory)
        {
            _logger = LoggingSource.Instance.GetLogger<LuceneIndexPersistence>(index.DocumentDatabase.Name);
            _suggestionsDirectories = new Dictionary<string, LuceneVoronDirectory>();
            _disposeOnce = new DisposeOnce<SingleAttempt>(() =>
            {
                DisposeWriters();

                _lastReader?.Dispose();
                _converter?.Dispose();
                _directory?.Dispose();

                foreach (var directory in _suggestionsDirectories)
                {
                    directory.Value?.Dispose();
                }

                TempFileCache?.Dispose();
            });

            var fields = index.Definition.IndexFields.Values;

            switch (_index.Type)
            {
                case IndexType.AutoMap:
                    _converter = new LuceneDocumentConverter(index, indexEmptyEntries: true);
                    break;
                case IndexType.AutoMapReduce:
                    _converter = new LuceneDocumentConverter(index, indexEmptyEntries: true, storeValue: true);
                    break;
                case IndexType.MapReduce:
                    _converter = new AnonymousLuceneDocumentConverter(index, storeValue: true);
                    break;
                case IndexType.Map:
                    switch (_index.SourceType)
                    {
                        case IndexSourceType.Documents:
                            _converter = new AnonymousLuceneDocumentConverter(index);
                            break;
                        case IndexSourceType.TimeSeries:
                        case IndexSourceType.Counters:
                            _converter = new CountersAndTimeSeriesAnonymousLuceneDocumentConverter(index);
                            break;
                    }
                    break;
                case IndexType.JavaScriptMap:
                    switch (_index.SourceType)
                    {
                        case IndexSourceType.Documents:
                            _converter = new JintLuceneDocumentConverter((MapIndex)index);
                            break;
                        case IndexSourceType.TimeSeries:
                            _converter = new CountersAndTimeSeriesJintLuceneDocumentConverter((MapTimeSeriesIndex)index);
                            break;
                        case IndexSourceType.Counters:
                            _converter = new CountersAndTimeSeriesJintLuceneDocumentConverter((MapCountersIndex)index);
                            break;
                    }
                    break;
                case IndexType.JavaScriptMapReduce:
                    _converter = new JintLuceneDocumentConverter((MapReduceIndex)index, storeValue: true);
                    break;
                case IndexType.Faulty:
                    _converter = null;
                    break;
                default:
                    throw new NotSupportedException(_index.Type.ToString());
            }

            _fields = fields.ToDictionary(x => x.Name, x => x);
        }

        [ThreadStatic]
        private static IState _currentIndexState;
        
        private IndexSearcher CreateIndexSearcher()
        {
            lock (_readersLock)
            {
                IState state = _currentIndexState;
                var reader = _lastReader;

                if (reader != null)
                {
                    if (reader.RefCount <= 0)
                    {
                        reader = null;
                    }
                    else
                    {
                        try
                        {
                            var newReader = reader.Reopen(state);
                            if (newReader != reader)
                            {
                                reader.DecRef(state);
                            }

                            reader = _lastReader = newReader;
                        }
                        catch (Exception e)
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info($"Could not reopen the index reader for index '{_index.Name}'.", e);

                            // fallback strategy in case of a reader to be closed
                            // before Reopen and DecRef are executed
                            reader = null;
                        }
                    }
                }

                reader ??= _lastReader = IndexReader.Open(_directory, deletionPolicy: null, readOnly: true, termInfosIndexDivisor: _index.Configuration.ReaderTermsIndexDivisor, state);

                reader.IncRef();

                return new IndexSearcher(reader);
            }
        }

        private bool _indexWriterCleanupNeeded;

        public override void CleanWritersIfNeeded()
        {
            if (_indexWriterCleanupNeeded == false)
                return;

            DisposeWriters();
            _indexWriterCleanupNeeded = false;
        }

        public override void Clean(IndexCleanup mode)
        {
            Debug.Assert(mode != IndexCleanup.None, "mode != IndexCleanup.None");

            _converter?.Clean();
            if (mode.HasFlag(IndexCleanup.Readers))
            {
                foreach (LowLevelTransaction llt in _index._indexStorage.Environment().ActiveTransactions.Enumerate())
                {
                    if (llt.TryGetClientState(out IndexStateRecord stateRecord))
                    {
                        stateRecord.LuceneIndexState.Recreate(CreateIndexSearcher);
                    }
                    else
                    {
                        throw new InvalidOperationException("Unable to find index ClientState, should not be possible");
                    }
                }
            }

            if (mode.HasFlag(IndexCleanup.Writers))
            {
                if (_indexWriter != null)
                {
                    // schedule index run only if clean is really needed
                    _indexWriterCleanupNeeded = true;
                    _index.ScheduleIndexingRun();
                }
            }

            if (mode.HasFlag(IndexCleanup.Readers))
            {
                lock (_readersLock)
                {
                    _lastReader?.DecRef(null);
                    _lastReader = null;
                }
            }
        }
        public override void Initialize(StorageEnvironment environment)
        {
            if (_initialized)
                throw new InvalidOperationException();

            TempFileCache = new TempFileCache(environment.Options);

            using (var tx = environment.WriteTransaction())
            {
                InitializeMainIndexStorage(tx, environment);
                InitializeSuggestionsIndexStorage(tx, environment);
                
                tx.LowLevelTransaction.UpdateClientState(UpdateIndexCache(tx));

                // force tx commit so it will bump tx counter and just created searcher holder will have valid tx id
                tx.LowLevelTransaction.ModifyPage(0);

                tx.Commit();
            }

            _initialized = true;
        }

        public override bool RequireOnBeforeExecuteIndexing()
        {
            return false;
        }

        public override void OnBeforeExecuteIndexing(IndexingStatsAggregator indexingStatsAggregator, CancellationToken token)
        {
        }

        public override IndexStateRecord UpdateIndexCache(Transaction tx)
        {
            var dirs = ImmutableDictionary.CreateBuilder<string, ImmutableDictionary<string, Tree.ChunkDetails[]>>();
            dirs[_directory.Name] = GetLuceneFilesChunks(tx, _directory.Name);

            foreach (var (name, _) in _suggestionsDirectories)
            {
                dirs[name] = GetLuceneFilesChunks(tx, name);
            }

            if(tx.LowLevelTransaction.TryGetClientState(out IndexStateRecord rec) is false)
                rec = IndexStateRecord.Empty;

            return rec with { Collections = GetCollectionEtags(tx), DirectoriesByName = dirs.ToImmutable() };
        }
        
        private  ImmutableDictionary<string, IndexStateRecord.CollectionEtags> GetCollectionEtags(Transaction tx)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, IndexStateRecord.CollectionEtags>(); 
            AbstractStaticIndexBase compiled = null;

            if (_index.Type.IsStatic())
            {
                switch (_index)
                {
                    case MapIndex mapIndex:
                        compiled = mapIndex._compiled;
                        break;
                    case MapReduceIndex mapReduceIndex:
                        compiled = mapReduceIndex._compiled;
                        break;
                    case MapCountersIndex mapCountersIndex:
                        compiled = mapCountersIndex._compiled;
                        break;
                    case MapTimeSeriesIndex mapTimeSeriesIndex:
                        compiled = mapTimeSeriesIndex._compiled;
                        break;
                }
            }
            var referencedCollections = _index.GetReferencedCollections();

            foreach (string collection in _index.Collections)
            {
                using (Slice.From(tx.LowLevelTransaction.Allocator, collection, out Slice collectionSlice))
                {
                    var collectionEtags = compiled?.CollectionsWithCompareExchangeReferences.Contains(collection) == true
                        ? new IndexStateRecord.ReferenceCollectionEtags
                        {
                            LastEtag = _index._indexStorage.ReferencesForCompareExchange
                                .ReadLastProcessedReferenceEtag(tx, collection, IndexStorage.CompareExchangeReferences.CompareExchange),
                            LastProcessedTombstoneEtag = _index._indexStorage.ReferencesForCompareExchange
                                .ReadLastProcessedReferenceTombstoneEtag(tx, collection, IndexStorage.CompareExchangeReferences.CompareExchange)
                        }
                        : null;

                    var lastReferenceEtags = ImmutableDictionary.CreateBuilder<string, IndexStateRecord.ReferenceCollectionEtags>();
                    if (referencedCollections?.TryGetValue(collection, out var collectionNames) ==true && collectionNames.Count > 0)
                    {
                        foreach (var collectionName in collectionNames)
                        {
                            lastReferenceEtags[collectionName.Name] = new IndexStateRecord.ReferenceCollectionEtags
                            {
                                LastEtag = _index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(tx, collection, collectionName),
                                LastProcessedTombstoneEtag = _index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceTombstoneEtag(tx, collection, collectionName),
                            };
                        }
                    }

                    var etags = new IndexStateRecord.CollectionEtags(
                        IndexStorage.ReadLastEtag(tx,
                            IndexStorage.IndexSchema.EtagsTree,
                            collectionSlice
                        ),
                        IndexStorage.ReadLastEtag(tx,
                            IndexStorage.IndexSchema.EtagsTombstoneTree,
                            collectionSlice
                        ),
                        collectionEtags,
                        lastReferenceEtags.ToImmutable());


                    builder[collection] = etags;
                }
            }

            return builder.ToImmutable();
        }

        private ImmutableDictionary<string, Tree.ChunkDetails[]> GetLuceneFilesChunks(Transaction tx, string name)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, Tree.ChunkDetails[]>();
            var filesTree = tx.ReadTree(name);
            if (filesTree == null)
                return builder.ToImmutable();
            using (var it = filesTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var chunkDetails = filesTree.ReadTreeChunks(it.CurrentKey, out _);
                        if (chunkDetails == null)
                            continue;
                        builder[it.CurrentKey.ToString()] = chunkDetails;
                    } while (it.MoveNext());
                }
            }

            return builder.ToImmutable();
        }
        
        private void InitializeSuggestionsIndexStorage(Transaction tx, StorageEnvironment environment)
        {
            var dic = ImmutableDictionary<string, LuceneIndexState>.Empty.ToBuilder();
            foreach (var field in _fields)
            {
                if (!field.Value.HasSuggestions)
                    continue;

                var directory = new LuceneVoronDirectory(tx, environment, TempFileCache, $"Suggestions-{field.Key}", _index.Configuration.LuceneIndexInput);
                _suggestionsDirectories[field.Key] = directory;

                using (directory.SetTransaction(tx, out IState state))
                {
                    CreateIndexStructure(directory, state);
                }
                
                var dir = _suggestionsDirectories[field.Key];
                dic.Add(field.Key, CreateSuggestions(dir));
            }

            if (tx.LowLevelTransaction.TryGetClientState(out IndexStateRecord stateRecord) is false)
                throw new InvalidOperationException("Unable to find index ClientState, should not be possible");
            tx.LowLevelTransaction.UpdateClientState(stateRecord with
            {
                LuceneSuggestionStates = dic.ToImmutable()
            });
        }

        private static LuceneIndexState CreateSuggestions(Directory dir) => new(() => new IndexSearcher(dir, true, _currentIndexState));

        private void InitializeMainIndexStorage(Transaction tx, StorageEnvironment environment)
        {
            _directory = new LuceneVoronDirectory(tx, environment, TempFileCache, _index.Configuration.LuceneIndexInput);

            using (_directory.SetTransaction(tx, out IState state))
            {
                CreateIndexStructure(_directory, state);
                RecreateSearcher(tx);
            }
        }

        private void CreateIndexStructure(LuceneVoronDirectory directory, IState state)
        {
            new IndexWriter(directory, _dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED, state).Dispose();
        }

        public override IndexWriteOperationBase OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext)
        {
            CheckDisposed();
            CheckInitialized();

            if (_index.Type == IndexType.MapReduce || _index.Type == IndexType.JavaScriptMapReduce)
            {
                var mapReduceIndex = (MapReduceIndex)_index;
                if (string.IsNullOrWhiteSpace(mapReduceIndex.Definition.OutputReduceToCollection) == false)
                    return new OutputReduceLuceneIndexWriteOperation(mapReduceIndex, _directory, _converter, writeTransaction, this, indexContext);
            }

            return new LuceneIndexWriteOperation(
                _index,
                _directory,
                _converter,
                writeTransaction,
                this
                );
        }

        public override IndexReadOperationBase OpenIndexReader(Transaction readTransaction, IndexQueryServerSide query = null)
        {
            CheckDisposed();
            CheckInitialized();

            return IndexReadOperationFactory.CreateLuceneIndexReadOperation(_index, _directory, _index._queryBuilderFactories,
                readTransaction, query);
        }

        public override IndexFacetReadOperationBase OpenFacetedIndexReader(Transaction readTransaction)
        {
            CheckDisposed();
            CheckInitialized();

            return new LuceneIndexFacetedReadOperation(_index, _index.Definition, _directory, _index._queryBuilderFactories, readTransaction, _index._indexStorage.DocumentDatabase);
        }

        public override SuggestionIndexReaderBase OpenSuggestionIndexReader(Transaction readTransaction, string field)
        {
            CheckDisposed();
            CheckInitialized();

            if (readTransaction.LowLevelTransaction.TryGetClientState(out IndexStateRecord stateRecord) is false)
                throw new InvalidOperationException("Unable to find index ClientState, should not be possible");
            if (!_suggestionsDirectories.TryGetValue(field, out LuceneVoronDirectory directory) || 
                !stateRecord.LuceneSuggestionStates.TryGetValue(field, out var state))
                throw new InvalidOperationException($"No suggestions index found for field '{field}'.");
            IndexSearcher indexSearcherValue;
            _currentIndexState = new VoronState(readTransaction);
            try
            {
                indexSearcherValue = state.IndexSearcher.Value;
            }
            finally
            {
                _currentIndexState = null;
            }

            return IndexReadOperationFactory.CreateLuceneSuggestionIndexReader(_index, directory, readTransaction, indexSearcherValue);
        }

        internal override void RecreateSearcher(Transaction asOfTx)
        {
            if (asOfTx.LowLevelTransaction.TryGetClientState(out IndexStateRecord stateRecord) is false)
                stateRecord = IndexStateRecord.Empty;
            asOfTx.LowLevelTransaction.UpdateClientState(stateRecord with { LuceneIndexState = new LuceneIndexState(CreateIndexSearcher) });
        }

        internal override void RecreateSuggestionsSearchers(Transaction asOfTx)
        {
            if (_suggestionsDirectories.Count == 0)
                return;

            if (asOfTx.LowLevelTransaction.TryGetClientState(out IndexStateRecord record) is false)
                throw new InvalidOperationException("Unable to find index ClientState, should not be possible");
            var builder = record.LuceneSuggestionStates.ToBuilder();
            foreach (var suggestion in _suggestionsDirectories)
            {
                builder[suggestion.Key] = CreateSuggestions(suggestion.Value);
            }
            asOfTx.LowLevelTransaction.UpdateClientState(record with
            {
                LuceneSuggestionStates = builder.ToImmutable()
            });
        }

        internal LuceneIndexWriter EnsureIndexWriter(IState state)
        {
            if (_indexWriter != null)
                return _indexWriter;

            try
            {
                _snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
                return _indexWriter = new LuceneIndexWriter(_directory, StopAnalyzer, _snapshotter,
                    IndexWriter.MaxFieldLength.UNLIMITED, null, _index, state);
            }
            catch (Exception e) when (e.IsOutOfMemory() || e is DiskFullException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new IndexWriteException(e);
            }
        }

        internal Dictionary<string, LuceneSuggestionIndexWriter> EnsureSuggestionIndexWriter(IState state)
        {
            if (_suggestionsIndexWriters != null)
                return _suggestionsIndexWriters;

            _suggestionsIndexWriters = new Dictionary<string, LuceneSuggestionIndexWriter>();

            foreach (var item in _fields)
            {
                if (item.Value.HasSuggestions == false)
                    continue;

                string field = item.Key;

                try
                {
                    var snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
                    var writer = new LuceneSuggestionIndexWriter(field, _suggestionsDirectories[field],
                                        snapshotter, IndexWriter.MaxFieldLength.UNLIMITED,
                        _index, state);

                    _suggestionsIndexWriters[field] = writer;
                }
                catch (Exception e) when (e.IsOutOfMemory() || e is DiskFullException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    throw new IndexWriteException(e);
                }
            }

            return _suggestionsIndexWriters;
        }

        public override bool ContainsField(string field)
        {
            if (field == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                return _index.Type.IsMap();

            return _fields.ContainsKey(field);
        }

        public override void DisposeWriters()
        {
            _indexWriter?.Analyzer?.Dispose();
            _indexWriter?.Dispose();
            _indexWriter = null;

            if (_suggestionsIndexWriters != null)
            {
                foreach (var writer in _suggestionsIndexWriters)
                {
                    writer.Value?.Dispose();
                }

                _suggestionsIndexWriters = null;
            }
        }

        public override void Dispose()
        {
            _disposeOnce.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed()
        {
            if (_disposeOnce.Disposed)
                throw new ObjectDisposedException($"Index persistence for index '{_index.Definition.Name}' was already disposed.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckInitialized()
        {
            if (_initialized == false)
                throw new InvalidOperationException($"Index persistence for index '{_index.Definition.Name}' was not initialized.");
        }

        public override void AssertCanOptimize()
        {
            
        }
        
        public override void AssertCanDump()
        {
            
        }

        public IndexSearcher GetSearcher(Transaction tx, IState state)
        {
            _currentIndexState = state;
            try
            {
                if (tx.LowLevelTransaction.TryGetClientState(out IndexStateRecord record) is false)
                    throw new InvalidOperationException("Unable to find index ClientState, should not be possible");
                return record.LuceneIndexState.IndexSearcher.Value;
            }
            finally
            {
                _currentIndexState = null;
            }
        }
    }
}

