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
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;

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

        private SnapshotDeletionPolicy _snapshotter;

        private LuceneVoronDirectory _directory;

        private readonly IndexSearcherHolder _indexSearcherHolder;

        private bool _disposed;

        private bool _initialized;
        private Dictionary<string, object> _fields;

        public LuceneIndexPersistence(Index index)
        {
            _index = index;

            var fields = index.Definition.MapFields.Values.ToList();

            switch (_index.Type)
            {
                case IndexType.AutoMap:
                    _converter = new LuceneDocumentConverter(fields, reduceOutput: false);
                    break;
                case IndexType.AutoMapReduce:
                    var autoMapReduceIndexDefinition = (AutoMapReduceIndexDefinition)_index.Definition;
                    fields.AddRange(autoMapReduceIndexDefinition.GroupByFields.Values);

                    _converter = new LuceneDocumentConverter(fields, reduceOutput: true);
                    break;
                case IndexType.MapReduce:
                case IndexType.Map:
                    _converter = new AnonymousLuceneDocumentConverter(fields, _index.IsMultiMap, reduceOutput: _index.Type.IsMapReduce());
                    break;
                case IndexType.Faulty:
                    _converter = null;
                    break;
                default:
                    throw new NotSupportedException(_index.Type.ToString());
            }

            _fields = fields.ToDictionary(x => IndexField.ReplaceInvalidCharactersInFieldName(x.Name), x => (object)null);
            _indexSearcherHolder = new IndexSearcherHolder(state => new IndexSearcher(_directory, true, state), _index._indexStorage.DocumentDatabase);
        }

        public void Clean()
        {
            _converter?.Clean();
            _indexSearcherHolder.Cleanup(_index._indexStorage.Environment().PossibleOldestReadTransaction);
        }

        public void Initialize(StorageEnvironment environment)
        {
            if (_initialized)
                throw new InvalidOperationException();

            _directory = new LuceneVoronDirectory(environment);

            using (var tx = environment.WriteTransaction())
            {
                IState state;
                using (_directory.SetTransaction(tx, out state))
                {
                    CreateIndexStructure(state);
                    RecreateSearcher(tx);

                    // force tx commit so it will bump tx counter and just created searcher holder will have valid tx id
                    tx.LowLevelTransaction.ModifyPage(0); 
                }

                tx.Commit();
            }

            _initialized = true;
        }

        private void CreateIndexStructure(IState state)
        {
            new IndexWriter(_directory, _dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED, state).Dispose();
        }

        public IndexWriteOperation OpenIndexWriter(Transaction writeTransaction)
        {
            CheckDisposed();
            CheckInitialized();

            if (_index.Type == IndexType.MapReduce)
            {
                var mapReduceIndex = (MapReduceIndex) _index;
                if (string.IsNullOrWhiteSpace(mapReduceIndex.Definition.OutputReduceToCollection) == false)
                    return new OutputReduceIndexWriteOperation(mapReduceIndex, _directory, _converter, writeTransaction, this);
            }

            return new IndexWriteOperation(
                _index,
                _directory,
                _converter,
                writeTransaction,
                this // TODO arek - 'this' :/
                );
        }

        public IndexReadOperation OpenIndexReader(Transaction readTransaction)
        {
            CheckDisposed();
            CheckInitialized();

            return new IndexReadOperation(_index, _directory, _indexSearcherHolder, readTransaction);
        }

        public IndexFacetedReadOperation OpenFacetedIndexReader(Transaction readTransaction)
        {
            CheckDisposed();
            CheckInitialized();

            return new IndexFacetedReadOperation(_index.Definition.Name, _index.Definition.MapFields, _directory, _indexSearcherHolder, readTransaction, _index._indexStorage.DocumentDatabase);
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
                // TODO [ppekrol] support for IndexReaderWarmer?
                return _indexWriter = new LuceneIndexWriter(_directory, StopAnalyzer, _snapshotter,
                    IndexWriter.MaxFieldLength.UNLIMITED, null, _index._indexStorage.DocumentDatabase, state);
            }
            catch (Exception e)
            {
                throw new IndexWriteException(e);
            }
        }

        public bool ContainsField(string field)
        {
            if (field == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                return _index.Type.IsMap();

            field = FieldUtil.RemoveRangeSuffixIfNecessary(field);
            field = IndexField.ReplaceInvalidCharactersInFieldName(field);

            return _fields.ContainsKey(field);
        }

        public void Dispose()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(Index));

            _disposed = true;

            _indexWriter?.Analyzer?.Dispose();
            _indexWriter?.Dispose();
            _converter?.Dispose();
            _directory?.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index persistence for index '{_index.Definition.Name} ({_index.Etag})' was already disposed.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckInitialized()
        {
            if (_initialized == false)
                throw new InvalidOperationException($"Index persistence for index '{_index.Definition.Name} ({_index.Etag})' was not initialized.");
        }
    }
}