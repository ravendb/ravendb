using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Sparrow.Json;
using Sparrow.Threading;
using Voron;
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

        private LuceneVoronDirectory _directory;
        private readonly Dictionary<string, LuceneVoronDirectory> _suggestionsDirectories;
        private readonly Dictionary<string, IndexSearcherHolder> _suggestionsIndexSearcherHolders;

        private readonly IndexSearcherHolder _indexSearcherHolder;

        private DisposeOnce<SingleAttempt>_disposeOnce;

        private bool _initialized;
        private readonly Dictionary<string, IndexField> _fields;

        public LuceneIndexPersistence(Index index)
        {
            _index = index;
            _suggestionsDirectories = new Dictionary<string, LuceneVoronDirectory>();
            _suggestionsIndexSearcherHolders = new Dictionary<string, IndexSearcherHolder>();
            _disposeOnce = new DisposeOnce<SingleAttempt>(() =>
            {
                DisposeWriters();

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
                    _converter = new LuceneDocumentConverter(fields);
                    break;
                case IndexType.AutoMapReduce:
                    _converter = new LuceneDocumentConverter(fields, reduceOutput: true);
                    break;
                case IndexType.MapReduce:
                    _converter = new AnonymousLuceneDocumentConverter(fields, _index.IsMultiMap, reduceOutput: true);
                    break;
                case IndexType.Map:
                    _converter = new AnonymousLuceneDocumentConverter(fields, _index.IsMultiMap);
                    break;
                case IndexType.Faulty:
                    _converter = null;
                    break;
                default:
                    throw new NotSupportedException(_index.Type.ToString());
            }

            _fields = fields.ToDictionary(x => x.Name, x => x);
            _indexSearcherHolder = new IndexSearcherHolder(state => new IndexSearcher(_directory, true, state), _index._indexStorage.DocumentDatabase);

            foreach (var field in _fields)
            {
                if (!field.Value.HasSuggestions)
                    continue;

                string fieldName = field.Key;
                _suggestionsIndexSearcherHolders[fieldName] = new IndexSearcherHolder(state => new IndexSearcher(_suggestionsDirectories[fieldName], true, state), _index._indexStorage.DocumentDatabase);
            }
        }

        public void Clean()
        {
            _converter?.Clean();
            _indexSearcherHolder.Cleanup(_index._indexStorage.Environment().PossibleOldestReadTransaction(null));
        }

        public void Initialize(StorageEnvironment environment)
        {
            if (_initialized)
                throw new InvalidOperationException();

            using (var tx = environment.WriteTransaction())
            {
                InitializeMainIndexStorage(tx, environment);
                InitializeSuggestionsIndexStorage(tx, environment);

                // force tx commit so it will bump tx counter and just created searcher holder will have valid tx id
                tx.LowLevelTransaction.ModifyPage(0);

                tx.Commit();
            }

            _initialized = true;
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
                    RecreateSuggestionsSearcher(tx, field.Key);
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

            if (_index.Type == IndexType.MapReduce)
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

        internal IndexSearcherHolder RecreateSuggestionsSearcher(Transaction asOfTx, string fieldKey)
        {
            var holder = _suggestionsIndexSearcherHolders[fieldKey];
            holder.SetIndexSearcher(asOfTx);
            return holder;
        }

        internal void RecreateSearcher(Transaction asOfTx)
        {
            _indexSearcherHolder.SetIndexSearcher(asOfTx);
        }

        internal LuceneIndexWriter EnsureIndexWriter(IState state)
        {
            if (_indexWriter != null)
                return _indexWriter;

            try
            {
                _snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
                return _indexWriter = new LuceneIndexWriter(_directory, StopAnalyzer, _snapshotter,
                    IndexWriter.MaxFieldLength.UNLIMITED, null, _index._indexStorage.DocumentDatabase, state);
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
                if (!item.Value.HasSuggestions)
                    continue;

                string field = item.Key;

                try
                {
                    var snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
                    var writer = new LuceneSuggestionIndexWriter(field, _suggestionsDirectories[field],
                                        snapshotter, IndexWriter.MaxFieldLength.UNLIMITED,
                                        _index._indexStorage.DocumentDatabase, state);

                    _suggestionsIndexWriters[field] = writer;
                }
                catch (Exception e)
                {
                    throw new IndexWriteException(e);
                }
            }

            return _suggestionsIndexWriters;
        }

        public bool ContainsField(string field)
        {
            if (field == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                return _index.Type.IsMap();

            return _fields.ContainsKey(field);
        }

        public void DisposeWriters()
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
