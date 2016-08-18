using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
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
                case IndexType.AutoMapReduce:
                case IndexType.MapReduce:
                    if (_index.Type == IndexType.AutoMapReduce)
                    {
                        var autoMapReduceIndexDefinition = (AutoMapReduceIndexDefinition)_index.Definition;
                        fields.AddRange(autoMapReduceIndexDefinition.GroupByFields.Values);
                    }
                    
                    _converter = new LuceneDocumentConverter(fields, reduceOutput: _index.Type.IsMapReduce());
                    break;
                case IndexType.Map:
                    _converter = new AnonymousLuceneDocumentConverter(fields);
                    break;
                case IndexType.Faulty:
                    _converter = null;
                    break;
                default:
                    throw new NotSupportedException(_index.Type.ToString());
            }

            _fields = fields.ToDictionary(x => IndexField.ReplaceInvalidCharactersInFieldName(x.Name), x => (object)null);
            _indexSearcherHolder = new IndexSearcherHolder(() => new IndexSearcher(_directory, true));
        }

        public void Initialize(StorageEnvironment environment, IndexingConfiguration configuration)
        {
            if (_initialized)
                throw new InvalidOperationException();

            if (_initialized)
                throw new InvalidOperationException();

            _directory = new LuceneVoronDirectory(environment);

            using (var tx = environment.WriteTransaction())
            {
                using (_directory.SetTransaction(tx))
                {
                    CreateIndexStructure();
                    RecreateSearcher();
                }

                tx.Commit();
            }

            _initialized = true;
        }

        private void CreateIndexStructure()
        {
            new IndexWriter(_directory, _dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Dispose();
        }

        public IndexWriteOperation OpenIndexWriter(Transaction writeTransaction)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index persistence for index '{_index.Definition.Name} ({_index.IndexId})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index persistence for index '{_index.Definition.Name} ({_index.IndexId})' was not initialized.");

            return new IndexWriteOperation(_index.Definition.Name, _index.Definition.MapFields, _directory, _converter, writeTransaction, this, _index._indexStorage.DocumentDatabase); // TODO arek - 'this' :/
        }

        public IndexReadOperation OpenIndexReader(Transaction readTransaction)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index persistence for index '{_index.Definition.Name} ({_index.IndexId})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index persistence for index '{_index.Definition.Name} ({_index.IndexId})' was not initialized.");

            return new IndexReadOperation(_index.Definition.Name, _index.Type, _index.MaxNumberOfIndexOutputs, _index.ActualMaxNumberOfIndexOutputs, _index.Definition.MapFields, _directory, _indexSearcherHolder, readTransaction, _index._indexStorage.DocumentDatabase);
        }

        internal void RecreateSearcher()
        {
            _indexSearcherHolder.SetIndexSearcher(wait: false);
        }

        internal LuceneIndexWriter EnsureIndexWriter()
        {
            if (_indexWriter != null)
                return _indexWriter;

            try
            {
                _snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
                // TODO [ppekrol] support for IndexReaderWarmer?
                return _indexWriter = new LuceneIndexWriter(_directory, StopAnalyzer, _snapshotter, 
                    IndexWriter.MaxFieldLength.UNLIMITED, null, _index._indexStorage.DocumentDatabase);
            }
            catch (Exception e)
            {
                throw new IndexWriteException(e);
            }
        }

        public bool ContainsField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

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
    }
}