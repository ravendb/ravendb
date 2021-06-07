// -----------------------------------------------------------------------
//  <copyright file="LuceneIndexWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Utils;
using Voron.Exceptions;
using Directory = Lucene.Net.Store.Directory;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class LuceneIndexWriter : IDisposable
    {
        private readonly Logger _logger;

        private TimeTrackingIndexWriter _indexWriter;

        private readonly LuceneVoronDirectory _directory;

        private readonly Analyzer _analyzer;

        private readonly IndexDeletionPolicy _indexDeletionPolicy;

        private readonly IndexWriter.MaxFieldLength _maxFieldLength;

        private readonly IndexWriter.IndexReaderWarmer _indexReaderWarmer;
        private readonly Index _index;

        public Analyzer Analyzer => _indexWriter?.Analyzer;

        public LuceneIndexWriter(LuceneVoronDirectory d, Analyzer a, IndexDeletionPolicy deletionPolicy,
            IndexWriter.MaxFieldLength mfl, IndexWriter.IndexReaderWarmer indexReaderWarmer, Index index, IState state)
        {
            _directory = d;
            _analyzer = a;
            _indexDeletionPolicy = deletionPolicy;
            _maxFieldLength = mfl;
            _indexReaderWarmer = indexReaderWarmer;
            _index = index;

            _logger = LoggingSource.Instance.GetLogger<LuceneIndexWriter>(index.DocumentDatabase.Name);
            RecreateIndexWriter(state);
        }

        public void AddDocument(global::Lucene.Net.Documents.Document doc, Analyzer a, IState state)
        {
            _indexWriter.AddDocument(doc, a, state);
        }

        public void DeleteDocuments(Term term, IState state)
        {
            _indexWriter.DeleteDocuments(term, state);
        }

        public int EntriesCount(IState state)
        {
            return _indexWriter.NumDocs(state);
        }

        public void Commit(IState state, IndexingStatsScope commitStats)
        {
            try
            {
                _indexWriter.SetCommitStats(commitStats);
                _indexWriter.Commit(state);
            }
            catch (SystemException e)
            {
                TryThrowingBetterException(e, _directory);
                throw;
            }
            finally
            {
                _indexWriter.SetCommitStats(null);
            }
        }

        public static void TryThrowingBetterException(SystemException e, LuceneVoronDirectory directory)
        {
            if (e.Message.StartsWith("this writer hit an OutOfMemoryError"))
                throw new OutOfMemoryException("Index writer hit OOM during commit", e);

            if (e.InnerException is VoronUnrecoverableErrorException)
                VoronUnrecoverableErrorException.Raise("Index data is corrupted", e);

            if (e.IsOutOfDiskSpaceException())
            {
                // this commit stage is written to the temp scratch buffers
                var fullPath = directory.TempFullPath;
                var driveInfo = DiskSpaceChecker.GetDiskSpaceInfo(fullPath);
                var freeSpace = driveInfo != null ? driveInfo.TotalFreeSpace.ToString() : "N/A";
                throw new DiskFullException($"There isn't enough space to commit the index to {fullPath}. " +
                                            $"Currently available space: {freeSpace}", e);
            }
        }

        public long RamSizeInBytes()
        {
            return _indexWriter.RamSizeInBytes();
        }

        public void Optimize(IState state)
        {
            try
            {
                _indexWriter.Optimize(state);
            }
            catch (SystemException e)
            {
                TryThrowingBetterException(e, _directory);

                throw;
            }
            finally
            {
                RecreateIndexWriter(state);
            }
        }

        public void RecreateIndexWriter(IState state)
        {
            try
            {
                DisposeIndexWriter();

                if (_indexWriter == null)
                    CreateIndexWriter(state);
            }
            catch (Exception e) when (e.IsOutOfMemory())
            {
                throw;
            }
            catch (Exception e)
            {
                throw new IndexWriterCreationException(e);
            }
        }

        private void CreateIndexWriter(IState state)
        {
            _indexWriter = new TimeTrackingIndexWriter(_directory, _analyzer, _indexDeletionPolicy, _maxFieldLength, state);
            _indexWriter.UseCompoundFile = false;
            _indexWriter.SetMergePolicy(new LogByteSizeMergePolicy(_indexWriter)
            {
                MaxMergeMB = _index.Configuration.MaximumSizePerSegment.GetValue(SizeUnit.Megabytes),
                MergeFactor = _index.Configuration.MergeFactor,
                LargeSegmentSizeMB = _index.Configuration.LargeSegmentSizeToMerge.GetValue(SizeUnit.Megabytes),
                NumberOfLargeSegmentsToMergeInSingleBatch = _index.Configuration.NumberOfLargeSegmentsToMergeInSingleBatch
            });

            if (_indexReaderWarmer != null)
            {
                _indexWriter.MergedSegmentWarmer = _indexReaderWarmer;
            }

            var scheduler = new TimeTrackingSerialMergeScheduler(_index);
            _indexWriter.InitializeMergeScheduler(scheduler, state);

            // RavenDB already manages the memory for those, no need for Lucene to do this as well
            _indexWriter.SetMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
            _indexWriter.SetRAMBufferSizeMB(1024);
        }

        private void DisposeIndexWriter()
        {
            if (_indexWriter == null)
                return;

            var writer = _indexWriter;
            _indexWriter = null;

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
                writer.Dispose(waitForMerges: false);
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

        public void AddIndexesNoOptimize(Directory[] directories, int count, IState state)
        {
            _indexWriter.AddIndexesNoOptimize(state, directories);
        }

        public int NumDocs(IState state)
        {
            return _indexWriter.NumDocs(state);
        }
    }
}
