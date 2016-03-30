using System;

using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;

using Raven.Server.Config.Categories;
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
        private readonly Analyzer _dummyAnalyzer = new SimpleAnalyzer();

        private readonly int _indexId;

        private readonly IndexDefinitionBase _definition;

        private readonly LuceneDocumentConverter _converter;

        private static readonly StopAnalyzer StopAnalyzer = new StopAnalyzer(Version.LUCENE_30);

        private readonly object _writeLock = new object();

        private LuceneIndexWriter _indexWriter;

        private SnapshotDeletionPolicy _snapshotter;

        private LuceneVoronDirectory _directory;

        private readonly IndexSearcherHolder _indexSearcherHolder = new IndexSearcherHolder();

        private bool _disposed;

        private bool _initialized;

        public LuceneIndexPersistence(int indexId, IndexDefinitionBase indexDefinition)
        {
            _indexId = indexId;
            _definition = indexDefinition;
            _converter = new LuceneDocumentConverter(_definition.MapFields.Values);
        }

        public void Initialize(StorageEnvironment environment, IndexingConfiguration configuration)
        {
            if (_initialized)
                throw new InvalidOperationException();

            lock (_writeLock)
            {
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
        }

        private void CreateIndexStructure()
        {
            new IndexWriter(_directory, _dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Dispose();
        }

        public IndexWriteOperation OpenIndexWriter(Transaction writeTransaction)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index persistance for index '{_definition.Name} ({_indexId})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index persistance for index '{_definition.Name} ({_indexId})' was not initialized.");

            return new IndexWriteOperation(_writeLock, _definition.Name, _definition.MapFields, _directory, _converter, writeTransaction, this); // TODO arek - 'this' :/
        }

        public IndexReadOperation OpenIndexReader(Transaction readTransaction)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index persistance for index '{_definition.Name} ({_indexId})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index persistance for index '{_definition.Name} ({_indexId})' was not initialized.");

            return new IndexReadOperation(_definition.Name, _definition.MapFields, _directory, _indexSearcherHolder, readTransaction);
        }

        internal void RecreateSearcher()
        {
            if (_indexWriter == null)
            {
                _indexSearcherHolder.SetIndexSearcher(new IndexSearcher(_directory, true), wait: false);
            }
            else
            {
                var indexReader = _indexWriter.GetReader();
                _indexSearcherHolder.SetIndexSearcher(new IndexSearcher(indexReader), wait: false);
            }
        }

        internal LuceneIndexWriter EnsureIndexWriter()
        {
            if (_indexWriter != null)
                return _indexWriter;

            try
            {
                _snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
                // TODO [ppekrol] support for IndexReaderWarmer?
                return _indexWriter = new LuceneIndexWriter(_directory, StopAnalyzer, _snapshotter, IndexWriter.MaxFieldLength.UNLIMITED, null);
            }
            catch (Exception e)
            {
                throw new IndexWriteException(e);
            }
        }

        public void Dispose()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(Index));

            lock (_writeLock)
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
}