using System;
using System.IO;

using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Store;

using Raven.Server.Config.Categories;
using Directory = Lucene.Net.Store.Directory;
using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene
{
    public class LuceneIndexPersistance : IDisposable
    {
        private static readonly StopAnalyzer StopAnalyzer = new StopAnalyzer(Version.LUCENE_30);

        private readonly object _writeLock = new object();

        private LuceneIndexWriter _indexWriter;

        private SnapshotDeletionPolicy _snapshotter;

        private Directory _directory;

        private bool _disposed;

        private bool _initialized;

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

        public void Write(Action<Action<global::Lucene.Net.Documents.Document>> addToIndex)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(LuceneIndexPersistance));

            if (_initialized == false)
                throw new InvalidOperationException();

            lock (_writeLock)
            {
                Analyzer analyzer = null;

                try
                {
                    analyzer = new LowerCaseKeywordAnalyzer();

                    EnsureIndexWriter();

                    var locker = _directory.MakeLock("writing-to-index.lock");
                    try
                    {
                        try
                        {
                            if (locker.Obtain() == false)
                            {
                                throw new InvalidOperationException();
                            }

                            addToIndex(document => AddDocumentToIndex(_indexWriter, document, analyzer));
                        }
                        catch (Exception)
                        {
                            throw;
                        }

                        if (_indexWriter != null && _indexWriter.RamSizeInBytes() >= long.MaxValue)
                        {
                            Flush(); // just make sure changes are flushed to disk
                        }
                    }
                    finally
                    {
                        locker.Release();
                    }
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    analyzer?.Close();
                }
            }
        }

        private static void AddDocumentToIndex(LuceneIndexWriter indexWriter, global::Lucene.Net.Documents.Document luceneDoc, Analyzer analyzer)
        {
            indexWriter.AddDocument(luceneDoc, analyzer);

            foreach (var fieldable in luceneDoc.GetFields())
            {
                using (fieldable.ReaderValue) // dispose all the readers
                {
                }
            }
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
    }
}