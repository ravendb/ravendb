using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Store;

using Raven.Abstractions.Data;
using Raven.Server.Config.Categories;
using Directory = Lucene.Net.Store.Directory;
using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene
{
    public class LuceneIndexPersistance : IDisposable
    {
        private readonly int _indexId;

        private readonly string _indexName;

        private static readonly StopAnalyzer StopAnalyzer = new StopAnalyzer(Version.LUCENE_30);

        private readonly object _writeLock = new object();

        private LuceneIndexWriter _indexWriter;

        private SnapshotDeletionPolicy _snapshotter;

        private Directory _directory;

        private bool _disposed;

        private bool _initialized;

        public LuceneIndexPersistance(int indexId, string indexName)
        {
            _indexId = indexId;
            _indexName = indexName;
        }

        public void Initialize(IndexingConfiguration indexingConfiguration)
        {
            if (_initialized)
                throw new InvalidOperationException();

            lock (_writeLock)
            {
                if (_initialized)
                    throw new InvalidOperationException();

                if (indexingConfiguration.RunInMemory)
                {
                    _directory = new RAMDirectory();
                }
                else
                {
                    throw new NotImplementedException();
                }

                _initialized = true;
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
                _directory?.Dispose();
            }
        }

        public IIndexActions Open()
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index persistance for index '{_indexName} ({_indexId})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index persistance for index '{_indexName} ({_indexId})' was not initialized.");

            return new LuceneIndexActions(this);
        }

        private void Flush()
        {
            try
            {
                lock (_writeLock)
                {
                    if (_disposed)
                        return;
                    if (_indexWriter == null)
                        return;

                    _indexWriter.Commit();
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        private void CreateIndexWriter()
        {
            try
            {
                _snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
                _indexWriter = new LuceneIndexWriter(_directory, StopAnalyzer, _snapshotter, IndexWriter.MaxFieldLength.UNLIMITED, 1024, null);
            }
            catch (Exception)
            {
                throw;
            }
        }

        private void EnsureIndexWriter()
        {
            try
            {
                if (_indexWriter == null)
                    CreateIndexWriter();
            }
            catch (IOException)
            {
                throw;
            }
        }

        private class LuceneIndexActions : IIndexActions
        {
            private readonly Term _documentId = new Term(Constants.DocumentIdFieldName, "Dummy");

            private readonly LuceneIndexPersistance _persistance;

            private readonly LowerCaseKeywordAnalyzer _analyzer;

            private readonly Lock _locker;

            public LuceneIndexActions(LuceneIndexPersistance persistance)
            {
                _persistance = persistance;

                Monitor.Enter(_persistance._writeLock);

                _analyzer = new LowerCaseKeywordAnalyzer();

                _persistance.EnsureIndexWriter();

                _locker = _persistance._directory.MakeLock("writing-to-index.lock");

                if (_locker.Obtain() == false)
                    throw new InvalidOperationException();
            }

            public void Dispose()
            {
                try
                {
                    if (_persistance._indexWriter != null && _persistance._indexWriter.RamSizeInBytes() >= long.MaxValue)
                        _persistance.Flush(); // just make sure changes are flushed to disk
                }
                finally
                {
                    _locker?.Release();
                    _analyzer?.Dispose();

                    Monitor.Exit(_persistance._writeLock);
                }
            }

            public void Write(global::Lucene.Net.Documents.Document document)
            {
                _persistance._indexWriter.AddDocument(document, _analyzer);

                foreach (var fieldable in document.GetFields())
                {
                    using (fieldable.ReaderValue) // dispose all the readers
                    {
                    }
                }
            }

            public void Delete(string key)
            {
                _persistance._indexWriter.DeleteDocuments(_documentId.CreateTerm(key));
            }
        }
    }
}