// -----------------------------------------------------------------------
//  <copyright file="LuceneIndexWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;

using Raven.Abstractions.Logging;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class LuceneIndexWriter : IDisposable
    {
        private static Logger _logger;

        private static DocumentDatabase _documentDatabase;

        private IndexWriter indexWriter;

        private readonly Directory directory;

        private readonly Analyzer analyzer;

        private readonly IndexDeletionPolicy indexDeletionPolicy;

        private readonly IndexWriter.MaxFieldLength maxFieldLength;

        private readonly IndexWriter.IndexReaderWarmer _indexReaderWarmer;

        public Directory Directory => indexWriter?.Directory;

        public Analyzer Analyzer => indexWriter?.Analyzer;

        public LuceneIndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, 
            IndexWriter.MaxFieldLength mfl, IndexWriter.IndexReaderWarmer indexReaderWarmer, DocumentDatabase documentDatabase)
        {
            directory = d;
            analyzer = a;
            indexDeletionPolicy = deletionPolicy;
            maxFieldLength = mfl;
            _indexReaderWarmer = indexReaderWarmer;
            _documentDatabase = documentDatabase;
            _logger = _documentDatabase.LoggerSetup.GetLogger<LuceneIndexWriter>(documentDatabase.Name);
            RecreateIndexWriter();
        }

        public void AddDocument(global::Lucene.Net.Documents.Document doc)
        {
            indexWriter.AddDocument(doc);
        }

        public void AddDocument(global::Lucene.Net.Documents.Document doc, Analyzer a)
        {
            indexWriter.AddDocument(doc, a);
        }

        public void DeleteDocuments(Term term)
        {
            indexWriter.DeleteDocuments(term);
        }

        public void DeleteDocuments(Term[] terms)
        {
            indexWriter.DeleteDocuments(terms);
        }

        public void Commit()
        {
            try
            {
                indexWriter.Commit();
            }
            finally
            {
                RecreateIndexWriter();
            }
        }

        public long RamSizeInBytes()
        {
            return indexWriter.RamSizeInBytes();
        }

        public void Optimize()
        {
            indexWriter.Optimize();
        }

        private void RecreateIndexWriter()
        {
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
            indexWriter.SetMergeScheduler(new SerialMergeScheduler());

            // RavenDB already manages the memory for those, no need for Lucene to do this as well
            indexWriter.SetMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
            indexWriter.SetRAMBufferSizeMB(1024);
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
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error while closing the index (closing the analyzer failed)", e);
            }

            try
            {
                writer.Dispose(waitForMerges);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error when closing the index", e);
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
            return new LuceneIndexWriter(ramDirectory, analyzer, indexDeletionPolicy, maxFieldLength, _indexReaderWarmer, _documentDatabase);
        }

        public void AddIndexesNoOptimize(Directory[] directories, int count)
        {
            indexWriter.AddIndexesNoOptimize(directories);
        }

        public int NumDocs()
        {
            return indexWriter.NumDocs();
        }
    }
}
