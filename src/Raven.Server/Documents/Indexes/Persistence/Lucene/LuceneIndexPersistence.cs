using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Threading;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;

using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class LuceneIndexPersistence : IDisposable
    {
        private readonly Index _index;

        private readonly Analyzer _dummyAnalyzer = new SimpleAnalyzer();

        private readonly LuceneDocumentConverterBase _converter;

        private static readonly StopAnalyzer StopAnalyzer = new StopAnalyzer(Version.LUCENE_30);

        private LuceneIndexWriter _indexWriter;
        private Dictionary<string, LuceneSuggestionIndexWriter> _suggestionsIndexWriters;

        private SnapshotDeletionPolicy _snapshotter;

        // this is used to remember the positions of files in the database
        // always points to the latest valid transaction and is updated by
        // the write tx on commit, thread safety is inherited from the voron
        // transaction
        private IndexTransactionCache _streamsCache = new IndexTransactionCache();

        private LuceneVoronDirectory _directory;
        private readonly Dictionary<string, LuceneVoronDirectory> _suggestionsDirectories;
        private readonly Dictionary<string, IndexSearcherHolder> _suggestionsIndexSearcherHolders;

        private readonly IndexSearcherHolder _indexSearcherHolder;

        private readonly DisposeOnce<SingleAttempt> _disposeOnce;

        private bool _initialized;
        private readonly Dictionary<string, IndexField> _fields;
        internal IndexReader _lastReader;
        private readonly Logger _logger;

        internal LuceneVoronDirectory LuceneDirectory => _directory;

        private readonly object _readersLock = new object();
        private readonly object _writersLock = new object();

        public LuceneIndexPersistence(Index index)
        {
            _index = index;
            _logger = LoggingSource.Instance.GetLogger<LuceneIndexPersistence>(index.DocumentDatabase.Name);
            _suggestionsDirectories = new Dictionary<string, LuceneVoronDirectory>();
            _suggestionsIndexSearcherHolders = new Dictionary<string, IndexSearcherHolder>();
            _disposeOnce = new DisposeOnce<SingleAttempt>(() =>
            {
                DisposeWriters();

                _lastReader?.Dispose();
                _indexSearcherHolder?.Dispose();
                _converter?.Dispose();
                _directory?.Dispose();

                foreach (var directory in _suggestionsDirectories)
                {
                    directory.Value?.Dispose();
                }
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

            _indexSearcherHolder = new IndexSearcherHolder(CreateIndexSearcher, _index._indexStorage.DocumentDatabase);

            foreach (var field in _fields)
            {
                if (!field.Value.HasSuggestions)
                    continue;

                string fieldName = field.Key;
                _suggestionsIndexSearcherHolders[fieldName] = new IndexSearcherHolder(state => new IndexSearcher(_suggestionsDirectories[fieldName], true, state), _index._indexStorage.DocumentDatabase);
            }

            IndexSearcher CreateIndexSearcher(IState state)
            {
                lock (_readersLock)
                {
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
                                    reader.Dispose();
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

                    reader ??= _lastReader = IndexReader.Open(_directory, readOnly: true, state);

                    reader.IncRef();

                    return new IndexSearcher(reader);
                }
            }
        }
        
        public void Clean(IndexCleanup mode)
        {
            Debug.Assert(mode != IndexCleanup.None, "mode != IndexCleanup.None");

            _converter?.Clean();
            _indexSearcherHolder.Cleanup(_index._indexStorage.Environment().PossibleOldestReadTransaction(null), mode);

            if (mode.HasFlag(IndexCleanup.Writers))
                DisposeWriters();

            if (mode.HasFlag(IndexCleanup.Readers))
            {
                lock (_readersLock)
                {
                    _lastReader?.Dispose();
                    _lastReader = null;
                }
            }
        }

        public void Initialize(StorageEnvironment environment)
        {
            if (_initialized)
                throw new InvalidOperationException();

            environment.NewTransactionCreated += SetStreamCacheInTx;

            using (var tx = environment.WriteTransaction())
            {
                InitializeMainIndexStorage(tx, environment);
                InitializeSuggestionsIndexStorage(tx, environment);
                BuildStreamCacheAfterTx(tx);

                // force tx commit so it will bump tx counter and just created searcher holder will have valid tx id
                tx.LowLevelTransaction.ModifyPage(0);

                tx.Commit();
            }

            _initialized = true;
        }

        public void PublishIndexCacheToNewTransactions(IndexTransactionCache transactionCache)
        {
            _streamsCache = transactionCache;
        }

        internal IndexTransactionCache BuildStreamCacheAfterTx(Transaction tx)
        {
            var newCache = new IndexTransactionCache();

            FillCollectionEtags(tx, newCache.Collections);

            var directoryFiles = new IndexTransactionCache.DirectoryFiles();
            newCache.DirectoriesByName[_directory.Name] = directoryFiles;
            FillLuceneFilesChunks(tx, directoryFiles.ChunksByName, _directory.Name);

            foreach (var (name, _) in _suggestionsDirectories)
            {
                directoryFiles = new IndexTransactionCache.DirectoryFiles();
                newCache.DirectoriesByName[name] = directoryFiles;
                FillLuceneFilesChunks(tx, directoryFiles.ChunksByName, name);
            }

            return newCache;
        }

        private void FillCollectionEtags(Transaction tx,
            Dictionary<string, IndexTransactionCache.CollectionEtags> map)
        {
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

            foreach (string collection in _index.Collections)
            {
                using (Slice.From(tx.LowLevelTransaction.Allocator, collection, out Slice collectionSlice))
                {
                    var etags = new IndexTransactionCache.CollectionEtags
                    {
                        LastIndexedEtag = IndexStorage.ReadLastEtag(tx,
                            IndexStorage.IndexSchema.EtagsTree,
                            collectionSlice
                        ),
                        LastProcessedTombstoneEtag = IndexStorage.ReadLastEtag(tx,
                            IndexStorage.IndexSchema.EtagsTombstoneTree,
                            collectionSlice
                        )
                    };

                    if (compiled?.CollectionsWithCompareExchangeReferences.Contains(collection) == true)
                    {
                        etags.LastReferencedEtagsForCompareExchange = new IndexTransactionCache.ReferenceCollectionEtags
                        {
                            LastEtag = _index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceEtag(tx, collection, IndexStorage.CompareExchangeReferences.CompareExchange),
                            LastProcessedTombstoneEtag = _index._indexStorage.ReferencesForCompareExchange.ReadLastProcessedReferenceTombstoneEtag(tx, collection, IndexStorage.CompareExchangeReferences.CompareExchange)
                        };
                    }

                    map[collection] = etags;
                }
            }

            var referencedCollections = _index.GetReferencedCollections();
            if (referencedCollections == null || referencedCollections.Count == 0)
                return;

            foreach (var (src, collections) in referencedCollections)
            {
                var collectionEtags = map[src];
                collectionEtags.LastReferencedEtags ??= new Dictionary<string, IndexTransactionCache.ReferenceCollectionEtags>(StringComparer.OrdinalIgnoreCase);
                foreach (var collectionName in collections)
                {
                    collectionEtags.LastReferencedEtags[collectionName.Name] = new IndexTransactionCache.ReferenceCollectionEtags
                    {
                        LastEtag = _index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(tx, src, collectionName),
                        LastProcessedTombstoneEtag = _index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceTombstoneEtag(tx, src, collectionName),
                    };
                }
            }
        }

        private void FillLuceneFilesChunks(Transaction tx, Dictionary<string, Tree.ChunkDetails[]> cache, string name)
        {
            var filesTree = tx.ReadTree(name);
            if (filesTree == null)
                return;
            using (var it = filesTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var chunkDetails = filesTree.ReadTreeChunks(it.CurrentKey, out _);
                        if (chunkDetails == null)
                            continue;
                        cache[it.CurrentKey.ToString()] = chunkDetails;
                    } while (it.MoveNext());
                }
            }
        }

        private void SetStreamCacheInTx(LowLevelTransaction tx)
        {
            tx.ImmutableExternalState = _streamsCache;
        }

        private void InitializeSuggestionsIndexStorage(Transaction tx, StorageEnvironment environment)
        {
            foreach (var field in _fields)
            {
                if (!field.Value.HasSuggestions)
                    continue;

                var directory = new LuceneVoronDirectory(tx, environment, $"Suggestions-{field.Key}");
                _suggestionsDirectories[field.Key] = directory;

                using (directory.SetTransaction(tx, out IState state))
                {
                    CreateIndexStructure(directory, state);
                    var holder = _suggestionsIndexSearcherHolders[field.Key];
                    holder.SetIndexSearcher(tx);
                }
            }
        }

        private void InitializeMainIndexStorage(Transaction tx, StorageEnvironment environment)
        {
            _directory = new LuceneVoronDirectory(tx, environment);

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

        public IndexWriteOperation OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext)
        {
            CheckDisposed();
            CheckInitialized();

            if (_index.Type == IndexType.MapReduce || _index.Type == IndexType.JavaScriptMapReduce)
            {
                var mapReduceIndex = (MapReduceIndex)_index;
                if (string.IsNullOrWhiteSpace(mapReduceIndex.Definition.OutputReduceToCollection) == false)
                    return new OutputReduceIndexWriteOperation(mapReduceIndex, _directory, _converter, writeTransaction, this, indexContext);
            }

            return new IndexWriteOperation(
                _index,
                _directory,
                _converter,
                writeTransaction,
                this
                );
        }

        public IndexReadOperation OpenIndexReader(Transaction readTransaction)
        {
            CheckDisposed();
            CheckInitialized();

            return new IndexReadOperation(_index, _directory, _indexSearcherHolder, _index._queryBuilderFactories, readTransaction);
        }

        public IndexFacetedReadOperation OpenFacetedIndexReader(Transaction readTransaction)
        {
            CheckDisposed();
            CheckInitialized();

            return new IndexFacetedReadOperation(_index, _index.Definition, _directory, _indexSearcherHolder, _index._queryBuilderFactories, readTransaction, _index._indexStorage.DocumentDatabase);
        }

        public LuceneSuggestionIndexReader OpenSuggestionIndexReader(Transaction readTransaction, string field)
        {
            CheckDisposed();
            CheckInitialized();

            if (!_suggestionsDirectories.TryGetValue(field, out LuceneVoronDirectory directory))
                throw new InvalidOperationException($"No suggestions index found for field '{field}'.");

            if (!_suggestionsIndexSearcherHolders.TryGetValue(field, out IndexSearcherHolder holder))
                throw new InvalidOperationException($"No suggestions index found for field '{field}'.");

            return new LuceneSuggestionIndexReader(_index, directory, holder, readTransaction);
        }

        internal void RecreateSearcher(Transaction asOfTx)
        {
            _indexSearcherHolder.SetIndexSearcher(asOfTx);
        }

        internal void RecreateSuggestionsSearchers(Transaction asOfTx)
        {
            foreach (var suggestion in _suggestionsIndexSearcherHolders)
            {
                suggestion.Value.SetIndexSearcher(asOfTx);
            }
        }

        internal LuceneIndexWriter EnsureIndexWriter(IState state)
        {
            lock (_writersLock)
            {
                if (_indexWriter != null)
                    return _indexWriter;

                try
                {
                    _snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
                    return _indexWriter = new LuceneIndexWriter(_directory, StopAnalyzer, _snapshotter,
                        IndexWriter.MaxFieldLength.UNLIMITED, null, _index, state);
                }
                catch (Exception e) when (e.IsOutOfMemory())
                {
                    throw;
                }
                catch (Exception e)
                {
                    throw new IndexWriteException(e);
                }
            }
        }

        internal Dictionary<string, LuceneSuggestionIndexWriter> EnsureSuggestionIndexWriter(IState state)
        {
            lock (_writersLock)
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
                    catch (Exception e) when (e.IsOutOfMemory())
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
        }

        public bool ContainsField(string field)
        {
            if (field == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                return _index.Type.IsMap();

            return _fields.ContainsKey(field);
        }

        public void DisposeWriters()
        {
            lock (_writersLock)
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
        }

        public void Dispose()
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
    }
}
