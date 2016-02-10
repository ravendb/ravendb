using System;
using System.Collections.Generic;
using System.IO;

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;

using Raven.Server.Json;

using Directory = Lucene.Net.Store.Directory;
using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Indexes.Storage.Lucene
{
    public class LuceneIndexStorage : IDisposable
    {
        private static readonly StopAnalyzer StopAnalyzer = new StopAnalyzer(Version.LUCENE_30);

        private readonly object writeLock = new object();

        private LuceneIndexWriter _indexWriter;

        private SnapshotDeletionPolicy _snapshotter;

        private Directory _directory;

        private volatile bool _disposed;

        public LuceneIndexStorage()
        {
            DocumentConverter = new LuceneDocumentConverter();
        }

        public LuceneDocumentConverter DocumentConverter { get; private set; }

        public void Dispose()
        {
            lock (writeLock)
            {
                _disposed = true;
            }
        }

        public void Write(RavenOperationContext context, List<Document> documents)
        {
            if (_disposed)
                throw new ObjectDisposedException("LuceneIndexStorage was disposed.");

            lock (writeLock)
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

                            foreach (var document in documents)
                                AddDocumentToIndex(_indexWriter, document, analyzer);
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

        protected void AddDocumentToIndex(LuceneIndexWriter indexWriter, Document luceneDoc, Analyzer analyzer)
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
                lock (writeLock)
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