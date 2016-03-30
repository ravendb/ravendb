// -----------------------------------------------------------------------
//  <copyright file="LuceneIndexWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;

using Raven.Abstractions;
using Raven.Abstractions.Logging;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class LuceneIndexWriter : IDisposable
    {
        public const long CommitMarker = 0x1A4B93AB93BBC143;

        private static readonly ILog LogIndexing = LogManager.GetLogger(typeof(Field.Index).FullName + ".Indexing");

        private readonly int maximumNumberOfWritesBeforeRecreate;

        private IndexWriter indexWriter;

        private readonly Directory directory;

        private readonly Analyzer analyzer;

        private readonly IndexDeletionPolicy indexDeletionPolicy;

        private readonly IndexWriter.MaxFieldLength maxFieldLength;

        private int currentNumberOfWrites;

        private readonly IndexWriter.IndexReaderWarmer _indexReaderWarmer;

        private int changesSinceCommit;

        private readonly Field forceCommitField = new Field("__dummy_force_index_write", "forced", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
        private readonly Term forceCommitTerm = new Term("__dummy_force_index_write", "forced");
        private readonly global::Lucene.Net.Documents.Document forceCommitDoc;
        private DateTime lastCommitDataStoreTime;

        public Directory Directory
        {
            get
            {
                if (indexWriter != null)
                    return indexWriter.Directory;

                return null;
            }
        }

        public Analyzer Analyzer
        {
            get
            {
                if (indexWriter != null)
                    return indexWriter.Analyzer;

                return null;
            }
        }

        public LuceneIndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, IndexWriter.MaxFieldLength mfl, int maximumNumberOfWritesBeforeRecreate, IndexWriter.IndexReaderWarmer indexReaderWarmer)
        {
            directory = d;
            analyzer = a;
            indexDeletionPolicy = deletionPolicy;
            maxFieldLength = mfl;
            _indexReaderWarmer = indexReaderWarmer;
            this.maximumNumberOfWritesBeforeRecreate = maximumNumberOfWritesBeforeRecreate;

            forceCommitDoc = new global::Lucene.Net.Documents.Document();
            forceCommitDoc.Add(forceCommitField);

            RecreateIfNecessary(force: true);
        }

        public void AddDocument(global::Lucene.Net.Documents.Document doc)
        {
            indexWriter.AddDocument(doc);
            currentNumberOfWrites++;
            changesSinceCommit++;
        }

        public void AddDocument(global::Lucene.Net.Documents.Document doc, Analyzer a)
        {
            indexWriter.AddDocument(doc, a);
            currentNumberOfWrites++;
            changesSinceCommit++;
        }

        public void DeleteDocuments(Term term)
        {
            indexWriter.DeleteDocuments(term);

            currentNumberOfWrites += 2; // deletes are more expensive than additions
            changesSinceCommit++;
        }

        public void DeleteDocuments(Term[] terms)
        {
            indexWriter.DeleteDocuments(terms);

            currentNumberOfWrites += terms.Length * 2; // deletes are more expensive than writes
            changesSinceCommit++;
        }

        public IndexReader GetReader()
        {
            return indexWriter.GetReader();
        }

        public void Commit()
        {
            var commitData = new Dictionary<string, string>
                             {
                                 { "Marker", CommitMarker.ToString(CultureInfo.InvariantCulture) }
                             };

            var commitDataStored = false;

            if (changesSinceCommit == 0 && SystemTime.UtcNow - lastCommitDataStoreTime > TimeSpan.FromMinutes(10))
            {
                ForceCommitDataStore();
                commitDataStored = true;
            }
            else if (changesSinceCommit > 0)
                commitDataStored = true;

            try
            {
                indexWriter.Commit(commitData);

                if (commitDataStored)
                    lastCommitDataStoreTime = SystemTime.UtcNow;

                changesSinceCommit = 0;
            }
            catch (SystemException e)
            {
                if (e.Message.StartsWith("this writer hit an OutOfMemoryError"))
                    RecreateIfNecessary(force: true);

                throw;
            }

            RecreateIfNecessary(force: true); // TODO arek - temp solution for AccessViolationException problem when reading VoronIndexInput after the original transaction was already disposed
        }

        private void ForceCommitDataStore()
        {
            indexWriter.AddDocument(forceCommitDoc);
            indexWriter.DeleteDocuments(forceCommitTerm);
        }

        public long RamSizeInBytes()
        {
            return indexWriter.RamSizeInBytes();
        }

        public void Optimize()
        {
            indexWriter.Optimize();
        }

        private void RecreateIfNecessary(bool force)
        {
            if (force || currentNumberOfWrites >= maximumNumberOfWritesBeforeRecreate)
                DisposeIndexWriter();

            if (indexWriter == null)
                CreateIndexWriter();
        }

        private void CreateIndexWriter()
        {
            indexWriter = new IndexWriter(directory, analyzer, indexDeletionPolicy, maxFieldLength);
            if (_indexReaderWarmer != null)
            {
                indexWriter.MergedSegmentWarmer = _indexReaderWarmer;
            }
            using (indexWriter.MergeScheduler)
            {
            }
            indexWriter.SetMergeScheduler(new ErrorLoggingConcurrentMergeScheduler());

            // RavenDB already manages the memory for those, no need for Lucene to do this as well
            indexWriter.SetMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
            indexWriter.SetRAMBufferSizeMB(1024);

            currentNumberOfWrites = 0;
        }

        private void DisposeIndexWriter(bool waitForMerges = true)
        {
            if (indexWriter == null)
                return;

            var writer = indexWriter;
            indexWriter = null;

            try
            {
                writer.Analyzer.Close();
            }
            catch (Exception e)
            {
                LogIndexing.ErrorException("Error while closing the index (closing the analyzer failed)", e);
            }

            try
            {
                writer.Dispose(waitForMerges);
            }
            catch (Exception e)
            {
                LogIndexing.ErrorException("Error when closing the index", e);
            }
        }

        public void Dispose()
        {
            DisposeIndexWriter();
        }

        public void Dispose(bool waitForMerges)
        {
            DisposeIndexWriter(waitForMerges);
        }

        public LuceneIndexWriter CreateRamWriter()
        {
            var ramDirectory = new RAMDirectory();
            if (_indexReaderWarmer != null)
            {
                indexWriter.MergedSegmentWarmer = _indexReaderWarmer;
            }
            return new LuceneIndexWriter(ramDirectory, analyzer, indexDeletionPolicy, maxFieldLength, int.MaxValue, _indexReaderWarmer);
        }

        public void AddIndexesNoOptimize(Directory[] directories, int count)
        {
            indexWriter.AddIndexesNoOptimize(directories);
            currentNumberOfWrites += count;
        }

        public int NumDocs()
        {
            return indexWriter.NumDocs();
        }
    }
}
