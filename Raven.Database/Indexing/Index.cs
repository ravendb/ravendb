//-----------------------------------------------------------------------
// <copyright file="Index.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.UI;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Search.Vectorhighlight;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json.Linq;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Database.Util;
using Raven.Json.Linq;
using Constants = Raven.Abstractions.Data.Constants;
using Directory = Lucene.Net.Store.Directory;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.Indexing
{
    /// <summary>
    /// 	This is a thread safe, single instance for a particular index.
    /// </summary>
    public abstract class Index : IDisposable,ILowMemoryHandler
    {
        protected static readonly ILog logIndexing = LogManager.GetLogger(typeof(Index).FullName + ".Indexing");
        protected static readonly ILog logQuerying = LogManager.GetLogger(typeof(Index).FullName + ".Querying");

        private const long WriteErrorsLimit = 10;

        private readonly List<Document> currentlyIndexDocuments = new List<Document>();
        protected Directory directory;
        protected readonly IndexDefinition indexDefinition;
        private volatile string waitReason;
        private readonly long flushSize;
        private long writeErrors;
        // Users sometimes configure index outputs without realizing that we need to count on that for memory 
        // management. That can result in very small batch sizes, so we want to make sure that we don't trust
        // the user configuration, and use what is actually going on
        private int maxActualIndexOutput = 1;

        public IndexingPriority Priority { get; set; }
        /// <summary>
        /// Note, this might be written to be multiple threads at the same time
        /// We don't actually care for exact timing, it is more about general feeling
        /// </summary>
        private DateTime? lastQueryTime;

        private readonly ConcurrentDictionary<string, IIndexExtension> indexExtensions =
            new ConcurrentDictionary<string, IIndexExtension>();

        internal readonly int indexId;

        public int IndexId
        {
            get { return indexId; }
        }

        private readonly AbstractViewGenerator viewGenerator;
        protected readonly WorkContext context;

        private readonly object writeLock = new object();
        private volatile bool disposed;
        private RavenIndexWriter indexWriter;
        private SnapshotDeletionPolicy snapshotter;
        private readonly IndexSearcherHolder currentIndexSearcherHolder;

        private readonly ConcurrentDictionary<string, IndexingPerformanceStats> currentlyIndexing = new ConcurrentDictionary<string, IndexingPerformanceStats>();
        private readonly ConcurrentQueue<IndexingPerformanceStats> indexingPerformanceStats = new ConcurrentQueue<IndexingPerformanceStats>();
        private readonly static StopAnalyzer stopAnalyzer = new StopAnalyzer(Version.LUCENE_30);
        private bool forceWriteToDisk;

        [CLSCompliant(false)]
        protected Index(Directory directory, int id, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator, WorkContext context)
        {
            currentIndexSearcherHolder = new IndexSearcherHolder(id, context);
            if (directory == null) throw new ArgumentNullException("directory");
            if (indexDefinition == null) throw new ArgumentNullException("indexDefinition");
            if (viewGenerator == null) throw new ArgumentNullException("viewGenerator");

            this.indexId = id;
            this.indexDefinition = indexDefinition;
            this.viewGenerator = viewGenerator;
            this.context = context;
            logIndexing.Debug("Creating index for {0}", PublicName);
            this.directory = directory;
            flushSize = context.Configuration.FlushIndexToDiskSizeInMb * 1024 * 1024;
            _indexCreationTime = SystemTime.UtcNow;
            RecreateSearcher();

            MemoryStatistics.RegisterLowMemoryHandler(this);
        }

        [ImportMany]
        public OrderedPartCollection<AbstractAnalyzerGenerator> AnalyzerGenerators { get; set; }

        /// <summary>
        /// Whatever this is a map reduce index or not
        /// </summary>
        public abstract bool IsMapReduce { get; }

        public DateTime? LastQueryTime
        {
            get
            {
                return lastQueryTime;
            }
        }

        public DateTime LastIndexTime { get; set; }

        protected DateTime PreviousIndexTime { get; set; }

        public string IsOnRam
        {
            get
            {
                var ramDirectory = directory as RAMDirectory;
                if (ramDirectory == null)
                    return "false";
                try
                {
                    return "true (" + SizeHelper.Humane(ramDirectory.SizeInBytes()) + ")";
                }
                catch (AlreadyClosedException)
                {
                    return "false";
                }
            }
        }

        public string PublicName { get { return indexDefinition.Name; } }

        public bool IsTestIndex
        {
            get { return indexDefinition.IsTestIndex; }
        }

        public int? MaxIndexOutputsPerDocument
        {
            get
            {
                if (maxActualIndexOutput == 1)
                    return null;
                return maxActualIndexOutput;
            }
        }

        [CLSCompliant(false)]
        public volatile bool IsMapIndexingInProgress;
        private DateTime _indexCreationTime;

        protected IndexingPerformanceStats RecordCurrentBatch(string indexingStep, string operation, int itemsCount)
        {
            var performanceStats = new IndexingPerformanceStats
            {
                ItemsCount = itemsCount,
                Operation = indexingStep,
                Started = SystemTime.UtcNow,
                Operations = new BasePerformanceStats[0]
            };

            var lastStats = indexingPerformanceStats.LastOrDefault(x => x.Operation.Equals(operation, StringComparison.OrdinalIgnoreCase));

            if (lastStats != null)
                performanceStats.WaitingTimeSinceLastBatchCompleted = performanceStats.Started - lastStats.Completed;

            currentlyIndexing.AddOrUpdate(indexingStep, performanceStats, (s, stats) => performanceStats);

            return performanceStats;
        }

        protected void BatchCompleted(string indexingStep, string operation, int inputCount, int outputCount, List<BasePerformanceStats> operationStats)
        {
            IndexingPerformanceStats stats;
            if (currentlyIndexing.TryRemove(indexingStep, out stats))
            {
                stats.Completed = SystemTime.UtcNow;
                stats.Duration = stats.Completed - stats.Started;
                stats.Operation = operation;

                stats.InputCount = inputCount;
                stats.OutputCount = outputCount;
                stats.Operations = operationStats.ToArray();

                AddIndexingPerformanceStats(stats);
            }
        }

        public void AddIndexingPerformanceStats(IndexingPerformanceStats stats)
        {
            indexingPerformanceStats.Enqueue(stats);

            while (indexingPerformanceStats.Count > 25)
                indexingPerformanceStats.TryDequeue(out stats);
        }

        public void Dispose()
        {
            try
            {
                // this is here so we can give good logs in the case of a long shutdown process
                if (Monitor.TryEnter(writeLock, 100) == false)
                {
                    var localReason = waitReason;
                    if (localReason != null)
                        logIndexing.Warn("Waiting for {0} to complete before disposing of index {1}, that might take a while if the server is very busy",
                         localReason, PublicName);

                    Monitor.Enter(writeLock);
                }

                disposed = true;

                foreach (var indexExtension in indexExtensions)
                {
                    indexExtension.Value.Dispose();
                }

                if (currentIndexSearcherHolder != null)
                {
                    var item = currentIndexSearcherHolder.SetIndexSearcher(null, PublicName, wait: true);
                    if (item.WaitOne(TimeSpan.FromSeconds(5)) == false)
                    {
                        logIndexing.Warn("After closing the index searching, we waited for 5 seconds for the searching to be done, but it wasn't. Continuing with normal shutdown anyway.");
                    }
                }

                try
                {
                    EnsureIndexWriter();
                    ForceWriteToDisk();
                    WriteInMemoryIndexToDiskIfNecessary(GetLastEtagFromStats());
                }
                catch (Exception e)
                {
                    logIndexing.ErrorException("Error while writing in memory index to disk.", e);
                }

                if (indexWriter != null) // just in case, WriteInMemoryIndexToDiskIfNecessary recreates writer
                {
                    var writer = indexWriter;
                    indexWriter = null;

                    try
                    {
                        writer.Analyzer.Close();
                    }
                    catch (Exception e)
                    {
                        logIndexing.ErrorException("Error while closing the index (closing the analyzer failed)", e);
                    }

                    try
                    {
                        writer.Dispose();
                    }
                    catch (Exception e)
                    {
                        logIndexing.ErrorException("Error when closing the index", e);
                    }
                }

                try
                {
                    directory.Dispose();
                }
                catch (Exception e)
                {
                    logIndexing.ErrorException("Error when closing the directory", e);
                }
            }
            finally
            {
                Monitor.Exit(writeLock);
            }
        }

        public void EnsureIndexWriter()
        {
            try
            {
                if (indexWriter == null)
                    CreateIndexWriter();
            }
            catch ( IOException e )
            {
                string msg = string.Format("Error when trying to create the index writer for index '{0}'.", this.PublicName);
                throw new IOException( msg, e );
            }
        }

        public void Flush(Etag highestETag)
        {
            try
            {
                lock (writeLock)
                {
                    if (disposed)
                        return;
                    if (indexWriter == null)
                        return;
                    if (context.IndexStorage == null)
                        return;

                    try
                    {
                        waitReason = "Flush";
                        indexWriter.Commit(highestETag);

                        ResetWriteErrors();
                    }
                    finally
                    {
                        waitReason = null;
                    }
                }
            }
            catch (Exception e)
            {
                IncrementWriteErrors(e);
                throw new IOException("Error during flush for " + PublicName, e);
            }
        }

        public void MergeSegments()
        {
            lock (writeLock)
            {
                waitReason = "Merge / Optimize";
                try
                {
                    logIndexing.Info("Starting merge of {0}", PublicName);
                    var sp = Stopwatch.StartNew();
                    
                    EnsureIndexWriter();

                    indexWriter.Optimize();
                    logIndexing.Info("Done merging {0} - took {1}", PublicName, sp.Elapsed);

                    ResetWriteErrors();
                }
                catch (Exception e)
                {
                    IncrementWriteErrors(e);
                    throw;
                }
                finally
                {
                    waitReason = null;
                }
            }
        }

        public abstract IndexingPerformanceStats IndexDocuments(AbstractViewGenerator viewGenerator, IndexingBatch batch, IStorageActionsAccessor actions, DateTime minimumTimestamp, CancellationToken token);

        protected virtual IndexQueryResult RetrieveDocument(Document document, FieldsToFetch fieldsToFetch, ScoreDoc score)
        {
            return new IndexQueryResult
            {
                Score = score.Score,
                Key = document.Get(Constants.DocumentIdFieldName),
                Projection = (fieldsToFetch.IsProjection || fieldsToFetch.FetchAllStoredFields) ? CreateDocumentFromFields(document, fieldsToFetch) : null
            };
        }

        public static RavenJObject CreateDocumentFromFields(Document document, FieldsToFetch fieldsToFetch)
        {
            var documentFromFields = new RavenJObject();
            var fields = fieldsToFetch.Fields;
            if (fieldsToFetch.FetchAllStoredFields)
                fields = fields.Concat(document.GetFields().Select(x => x.Name));

            AddFieldsToDocument(document, new HashSet<string>(fields), documentFromFields);
            return documentFromFields;
        }

        protected static void AddFieldsToDocument(Document document, HashSet<string> fieldNames, RavenJObject documentFromFields)
        {
            foreach (var fldName in fieldNames)
            {
                if (fldName.EndsWith("_IsArray") ||
                    fldName.EndsWith("_Range") ||
                    fldName.EndsWith("_ConvertToJson"))
                    continue;

                var isArray = fldName + "_IsArray";
                foreach (var field in document.GetFields(fldName))
                {
                    var val = CreateProperty(field, document);
                    RavenJToken arrayToken;
                    var tryGetValue = documentFromFields.TryGetValue(field.Name, out arrayToken);
                    if (tryGetValue || document.GetField(isArray) != null)
                    {
                        var array = arrayToken as RavenJArray;
                        if (array == null)
                        {
                            documentFromFields[field.Name] = array =
                                (tryGetValue ? new RavenJArray { arrayToken } : new RavenJArray());
                        }
                        array.Add(val);
                    }
                    else
                    {
                        documentFromFields[field.Name] = val;
                    }
                }
            }
        }

        protected void InvokeOnIndexEntryDeletedOnAllBatchers(List<AbstractIndexUpdateTriggerBatcher> batchers, Term term)
        {
            if (!batchers.Any(batcher => batcher.RequiresDocumentOnIndexEntryDeleted)) return;
            // find all documents
            var key = term.Text;

            IndexSearcher searcher = null;
            using (GetSearcher(out searcher))
            {
                var collector = new GatherAllCollector();
                searcher.Search(new TermQuery(term), collector);
                var topDocs = collector.ToTopDocs();

                foreach (var scoreDoc in topDocs.ScoreDocs)
                {
                    var document = searcher.Doc(scoreDoc.Doc);
                    batchers.ApplyAndIgnoreAllErrors(
                        exception =>
                        {
                            logIndexing.WarnException(
                                string.Format(
                                    "Error when executed OnIndexEntryDeleted trigger for index '{0}', key: '{1}'",
                                    PublicName, key),
                                exception);
                            context.AddError(indexId, PublicName, key, exception, "OnIndexEntryDeleted Trigger");
                        },
                        trigger => trigger.OnIndexEntryDeleted(key, document));
                }
            }
        }

        private static RavenJToken CreateProperty(Field fld, Document document)
        {
            if (fld.IsBinary)
                return fld.GetBinaryValue();
            var stringValue = fld.StringValue;
            if (document.GetField(fld.Name + "_ConvertToJson") != null)
            {
                var val = RavenJToken.Parse(fld.StringValue) as RavenJObject;
                return val;
            }
            if (stringValue == Constants.NullValue)
                stringValue = null;
            if (stringValue == Constants.EmptyString)
                stringValue = string.Empty;
            return stringValue;
        }

        protected void Write(Func<RavenIndexWriter, Analyzer, IndexingWorkStats, IndexedItemsInfo> action, List<PerformanceStats> writePerformanceStats = null)
        {
            if (disposed)
                throw new ObjectDisposedException("Index " + PublicName + " has been disposed");

            Stopwatch extensionExecutionDuration = null;
            Stopwatch flushToDiskDuration = null;
            Stopwatch recreateSearcherDuration = null;

            if (writePerformanceStats != null)
            {
                extensionExecutionDuration = new Stopwatch();
                flushToDiskDuration = new Stopwatch();
                recreateSearcherDuration = new Stopwatch();
            }

            PreviousIndexTime = LastIndexTime;
            LastIndexTime = SystemTime.UtcNow;

            lock (writeLock)
            {
                bool shouldRecreateSearcher;
                var toDispose = new List<Action>();
                Analyzer searchAnalyzer = null;
                var itemsInfo = new IndexedItemsInfo(null);
                bool flushed = false;

                try
                {
                    waitReason = "Write";
                    try
                    {
                        searchAnalyzer = CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose);
                    }
                    catch (Exception e)
                    {
                        context.AddError(indexId, indexDefinition.Name, "Creating Analyzer", e, "Analyzer");
                        throw;
                    }

                    EnsureIndexWriter();

                    var locker = directory.MakeLock("writing-to-index.lock");
                    try
                    {
                        var stats = new IndexingWorkStats();
                        shouldRecreateSearcher = false;

                        try
                        {
                            if (locker.Obtain() == false)
                            {
                                throw new InvalidOperationException(
                                    string.Format("Could not obtain the 'writing-to-index' lock of '{0}' index",
                                        PublicName));
                            }

                            itemsInfo = action(indexWriter, searchAnalyzer, stats);
                            shouldRecreateSearcher = itemsInfo.ChangedDocs > 0;
                            foreach (var indexExtension in indexExtensions.Values)
                            {
                                using (StopwatchScope.For(extensionExecutionDuration, resetBeforeStart: true))
                                {
                                    indexExtension.OnDocumentsIndexed(currentlyIndexDocuments, searchAnalyzer);
                                }

                                IndexingOperation operation;
                                if (writePerformanceStats != null && Enum.TryParse(string.Format("Extension_{0}", indexExtension.Name), out operation))
                                {
                                    writePerformanceStats.Add(PerformanceStats.From(operation, extensionExecutionDuration.ElapsedMilliseconds));
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            //do not add error if this exception happens,
                            //since this exception can happen during normal code-flow
                            throw;
                        }
                        catch (Exception e)
                        {
                            var invalidSpatialShapeException = e as InvalidSpatialShapException;
                            var invalidDocId = (invalidSpatialShapeException == null) ?
                                                        null :
                                                        invalidSpatialShapeException.InvalidDocumentId;
                            context.AddError(indexId, indexDefinition.Name, invalidDocId, e, "Write");
                            throw;
                        }

                        if (itemsInfo.ChangedDocs > 0)
                        {
                            using (StopwatchScope.For(flushToDiskDuration))
                            {
                                WriteInMemoryIndexToDiskIfNecessary(itemsInfo.HighestETag);

                                if (indexWriter != null && indexWriter.RamSizeInBytes() >= flushSize)
                                {
                                    Flush(itemsInfo.HighestETag); // just make sure changes are flushed to disk
                                    flushed = true;
                                }
                            }

                            UpdateIndexingStats(context, stats);
                        }
                    }
                    finally
                    {
                        locker.Release();
                    }
                }
                catch (OperationCanceledException)
                {
                    //do not add error if this exception happens,
                    //since this exception can happen during normal code-flow
                    throw;
                }
                catch (Exception e)
                {
                    IncrementWriteErrors(e);

                    throw new InvalidOperationException("Could not properly write to index " + PublicName, e);
                }
                finally
                {
                    currentlyIndexDocuments.Clear();
                    if (searchAnalyzer != null)
                        searchAnalyzer.Close();
                    foreach (Action dispose in toDispose)
                    {
                        dispose();
                    }
                    waitReason = null;
                    LastIndexTime = SystemTime.UtcNow;
                }

                if (flushed)
                {
                    try
                    {
                        HandleCommitPoints(itemsInfo, GetCurrentSegmentsInfo());
                    }
                    catch (Exception e)
                    {
                        logIndexing.WarnException("Could not handle commit point properly, ignoring", e);
                    }
                }

                if (shouldRecreateSearcher)
                {
                    using (StopwatchScope.For(recreateSearcherDuration))
                    {
                        RecreateSearcher();
                    }
                }
            }

            if (writePerformanceStats != null)
            {
                writePerformanceStats.Add(PerformanceStats.From(IndexingOperation.Lucene_FlushToDisk, flushToDiskDuration.ElapsedMilliseconds));
                writePerformanceStats.Add(PerformanceStats.From(IndexingOperation.Lucene_RecreateSearcher, recreateSearcherDuration.ElapsedMilliseconds));
            }
        }

        private IndexSegmentsInfo GetCurrentSegmentsInfo()
        {
            if (directory is RAMDirectory)
                return null;

            return IndexStorage.GetCurrentSegmentsInfo(indexDefinition.Name, directory);
        }

        protected abstract void HandleCommitPoints(IndexedItemsInfo itemsInfo, IndexSegmentsInfo segmentsInfo);

        protected void UpdateIndexingStats(WorkContext workContext, IndexingWorkStats stats)
        {
            // we'll try this for ten times, and if we get concurrency conflict, we do NOT fail, we'll retry
            // if we can't run even after ten times, we just give up. The stats might be a bit out, but that is fine for us
            bool run = true;
            for (int i = 0; i < 10 && run; i++)
            {
                run = false;
                switch (stats.Operation)
                {
                    case IndexingWorkStats.Status.Map:
                        try
                        {
                            workContext.TransactionalStorage.Batch(accessor =>
                            {
                                accessor.Indexing.UpdateIndexingStats(indexId, stats);
                            });
                        }
                        catch (ConcurrencyException)
                        {
                            run = true;
                        }
                        break;
                    case IndexingWorkStats.Status.Reduce:
                        try
                        {
                            workContext.TransactionalStorage.Batch(accessor =>
                            {
                                accessor.Indexing.UpdateReduceStats(indexId, stats);
                            });
                        }
                        catch (ConcurrencyException)
                        {
                            run = true;
                        }
                        break;
                    case IndexingWorkStats.Status.Ignore:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (run)
                    Thread.Sleep(11);
            }
        }

        private void CreateIndexWriter()
        {
            try
            {
                snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
                IndexWriter.IndexReaderWarmer indexReaderWarmer = context.IndexReaderWarmers != null
                    ? new IndexReaderWarmersWrapper(indexDefinition.Name, context.IndexReaderWarmers)
                    : null;
                indexWriter = new RavenIndexWriter(directory, stopAnalyzer, snapshotter, IndexWriter.MaxFieldLength.UNLIMITED, context.Configuration.MaxIndexWritesBeforeRecreate, indexReaderWarmer);
            }
            catch (Exception e)
            {
                IncrementWriteErrors(e);
                throw new IOException("Failure to create index writer for " + PublicName, e);
            }
        }

        internal void WriteInMemoryIndexToDiskIfNecessary(Etag highestETag)
        {
            if (context.Configuration.RunInMemory ||
                context.IndexDefinitionStorage == null) // may happen during index startup
                return;

            var dir = indexWriter.Directory as RAMDirectory;
            if (dir == null)
                return;

            var stale = IsUpToDateEnoughToWriteToDisk(highestETag) == false;
            var toobig = dir.SizeInBytes() >= context.Configuration.NewIndexInMemoryMaxBytes;

            var tooOld = (SystemTime.UtcNow - _indexCreationTime) > context.Configuration.NewIndexInMemoryMaxTime;

            if (forceWriteToDisk || toobig || !stale || tooOld)
            {
                indexWriter.Commit(highestETag);
                var fsDir = context.IndexStorage.MakeRAMDirectoryPhysical(dir, indexDefinition);
                IndexStorage.WriteIndexVersion(fsDir, indexDefinition);
                directory = fsDir;

                indexWriter.Dispose(true);
                dir.Dispose();

                CreateIndexWriter();

                ResetWriteErrors();
            }
        }

        protected abstract bool IsUpToDateEnoughToWriteToDisk(Etag highestETag);

        public RavenPerFieldAnalyzerWrapper CreateAnalyzer(Analyzer defaultAnalyzer, ICollection<Action> toDispose, bool forQuerying = false)
        {
            toDispose.Add(defaultAnalyzer.Close);

            string value;
            if (indexDefinition.Analyzers.TryGetValue(Constants.AllFields, out value))
            {
                defaultAnalyzer = IndexingExtensions.CreateAnalyzerInstance(Constants.AllFields, value);
                toDispose.Add(defaultAnalyzer.Close);
            }
            var perFieldAnalyzerWrapper = new RavenPerFieldAnalyzerWrapper(defaultAnalyzer);
            foreach (var analyzer in indexDefinition.Analyzers)
            {
                Analyzer analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(analyzer.Key, analyzer.Value);
                toDispose.Add(analyzerInstance.Close);

                if (forQuerying)
                {
                    var customAttributes = analyzerInstance.GetType().GetCustomAttributes(typeof(NotForQueryingAttribute), false);
                    if (customAttributes.Length > 0)
                        continue;
                }

                perFieldAnalyzerWrapper.AddAnalyzer(analyzer.Key, analyzerInstance);
            }
            StandardAnalyzer standardAnalyzer = null;
            KeywordAnalyzer keywordAnalyzer = null;
            foreach (var fieldIndexing in indexDefinition.Indexes)
            {
                switch (fieldIndexing.Value)
                {
                    case FieldIndexing.NotAnalyzed:
                        if (keywordAnalyzer == null)
                        {
                            keywordAnalyzer = new KeywordAnalyzer();
                            toDispose.Add(keywordAnalyzer.Close);
                        }
                        perFieldAnalyzerWrapper.AddAnalyzer(fieldIndexing.Key, keywordAnalyzer);
                        break;
                    case FieldIndexing.Analyzed:
                        if (indexDefinition.Analyzers.ContainsKey(fieldIndexing.Key))
                            continue;
                        if (standardAnalyzer == null)
                        {
                            standardAnalyzer = new StandardAnalyzer(Version.LUCENE_29);
                            toDispose.Add(standardAnalyzer.Close);
                        }
                        perFieldAnalyzerWrapper.AddAnalyzer(fieldIndexing.Key, standardAnalyzer);
                        break;
                }
            }
            return perFieldAnalyzerWrapper;
        }

        protected IEnumerable<object> RobustEnumerationIndex(IEnumerator<object> input, List<IndexingFunc> funcs, IndexingWorkStats stats, Stopwatch linqExecutionDuration)
        {
            Action<Exception, object> onErrorFunc;
            return RobustEnumerationIndex(input, funcs, stats, out onErrorFunc, linqExecutionDuration);
        }

        protected IEnumerable<object> RobustEnumerationIndex(IEnumerator<object> input, List<IndexingFunc> funcs, IndexingWorkStats stats, out Action<Exception, object> onErrorFunc, Stopwatch linqExecutionDuration)
        {
            onErrorFunc = (exception, o) =>
                {
                    string docId = null;

                    var invalidSpatialException = exception as InvalidSpatialShapException;
                    if (invalidSpatialException != null)
                        docId = invalidSpatialException.InvalidDocumentId;

                    context.AddError(indexId, indexDefinition.Name, docId ?? TryGetDocKey(o), exception, "Map");

                    logIndexing.WarnException(
                    String.Format("Failed to execute indexing function on {0} on {1}", indexDefinition.Name, TryGetDocKey(o)), exception);

                    stats.IndexingErrors++;
                };

            return new RobustEnumerator(context.CancellationToken, context.Configuration.MaxNumberOfItemsToProcessInSingleBatch,
                beforeMoveNext: () => Interlocked.Increment(ref stats.IndexingAttempts),
                cancelMoveNext: () => Interlocked.Decrement(ref stats.IndexingAttempts),
                onError: onErrorFunc )
                {
                    MoveNextDuration = linqExecutionDuration
                }
                .RobustEnumeration(input, funcs);
        }

        protected IEnumerable<object> RobustEnumerationReduce(IEnumerator<object> input, IndexingFunc func, IndexingWorkStats stats, Stopwatch linqExecutionDuration)
        {
            // not strictly accurate, but if we get that many errors, probably an error anyway.
            return new RobustEnumerator(context.CancellationToken, context.Configuration.MaxNumberOfItemsToProcessInSingleBatch,
                beforeMoveNext: () => Interlocked.Increment(ref stats.ReduceAttempts),
                cancelMoveNext: () => Interlocked.Decrement(ref stats.ReduceAttempts),
                onError: (exception, o) =>
                {
                    var key = TryGetDocKey(o);

                    context.AddError(indexId,
                        indexDefinition.Name,
                        key,
                        exception,
                        "Reduce"
                        );
                    logIndexing.WarnException(
                        String.Format("Failed to execute indexing function on {0} on {1}", indexDefinition.Name,
                            key),
                        exception);

                    stats.ReduceErrors++;
                }) 
                {
                    MoveNextDuration = linqExecutionDuration
                }.RobustEnumeration(input, func);
        }

        // we don't care about tracking map/reduce stats here, since it is merely
        // an optimization step
        protected IEnumerable<object> RobustEnumerationReduceDuringMapPhase(IEnumerator<object> input, IndexingFunc func, Stopwatch reduceDuringMapLinqExecution)
        {
            // not strictly accurate, but if we get that many errors, probably an error anyway.
            return new RobustEnumerator(context.CancellationToken, context.Configuration.MaxNumberOfItemsToProcessInSingleBatch,
                onError: (exception, o) =>
                {
                    var keys = TryGetDocKeys(input, o);
                    var concatenatedKeys = string.Join(";", keys);

                    context.AddError(indexId,
                                     indexDefinition.Name,
                                    concatenatedKeys,
                                    exception,
                                    "Reduce"
                        );

                    logIndexing.WarnException(
                        String.Format("Failed to execute indexing function on {0} on {1}", indexDefinition.Name,
                                        concatenatedKeys),
                        exception);
                })
                {
                    MoveNextDuration = reduceDuringMapLinqExecution
                }.RobustEnumeration(input, func);
        }

        private static IEnumerable<string> TryGetDocKeys(IEnumerator<object> input, object current)
        {
            var keys = new HashSet<string>();
            var key = TryGetDocKey(current);

            if (string.IsNullOrEmpty(key) == false)
                keys.Add(key);
            else
            {
                input.Reset();
                while (input.MoveNext())
                {
                    key = TryGetDocKey(input.Current);
                    if (string.IsNullOrEmpty(key))
                        continue;

                    keys.Add(key);
                }
            }

            return keys;
        }

        public static string TryGetDocKey(object current)
        {
            var dic = current as DynamicJsonObject;
            if (dic == null)
                return null;
            object value = dic.GetValue(Constants.DocumentIdFieldName) ??
                           dic.GetValue(Constants.ReduceKeyFieldName);
            if (value != null)
                return value.ToString();
            return null;
        }

        public abstract void Remove(string[] keys, WorkContext context);

        internal IndexSearcherHolder.IndexSearcherHoldingState GetCurrentStateHolder()
        {
            return currentIndexSearcherHolder.GetCurrentStateHolder();
        }

        internal IDisposable GetSearcher(out IndexSearcher searcher)
        {
            return currentIndexSearcherHolder.GetSearcher(out searcher);
        }

        internal IDisposable GetSearcherAndTermsDocs(out IndexSearcher searcher, out RavenJObject[] termsDocs)
        {
            return currentIndexSearcherHolder.GetSearcherAndTermDocs(out searcher, out termsDocs);
        }

        private void RecreateSearcher()
        {
            if (indexWriter == null)
            {
                currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(directory, true), PublicName,wait: false);
            }
            else
            {
                var indexReader = indexWriter.GetReader();
                currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(indexReader), PublicName, wait: false);
            }
        }

        protected void AddDocumentToIndex(RavenIndexWriter currentIndexWriter, Document luceneDoc, Analyzer analyzer)
        {
            Analyzer newAnalyzer = AnalyzerGenerators.Aggregate(analyzer,
                                                                (currentAnalyzer, generator) =>
                                                                {
                                                                    Analyzer generateAnalyzer =
                                                                        generator.Value.GenerateAnalyzerForIndexing(PublicName, luceneDoc,
                                                                                                            currentAnalyzer);
                                                                    if (generateAnalyzer != currentAnalyzer &&
                                                                        currentAnalyzer != analyzer)
                                                                        currentAnalyzer.Close();
                                                                    return generateAnalyzer;
                                                                });

            try
            {
                if (indexExtensions.Count > 0)
                    currentlyIndexDocuments.Add(CloneDocument(luceneDoc));

                currentIndexWriter.AddDocument(luceneDoc, newAnalyzer);

                foreach (var fieldable in luceneDoc.GetFields())
                {
                    using (fieldable.ReaderValue) // dispose all the readers
                    {

                    }
                }
            }
            finally
            {
                if (newAnalyzer != analyzer)
                    newAnalyzer.Close();
            }
        }

        public void MarkQueried()
        {
            lastQueryTime = SystemTime.UtcNow;
        }

        public void MarkQueried(DateTime time)
        {
            if (lastQueryTime != null &&
                lastQueryTime.Value >= time)
                return;

            lastQueryTime = time;
        }

        public IIndexExtension GetExtension(string indexExtensionKey)
        {
            IIndexExtension val;
            indexExtensions.TryGetValue(indexExtensionKey, out val);
            return val;
        }

        public IIndexExtension GetExtensionByPrefix(string indexExtensionKeyPrefix)
        {
            return indexExtensions.FirstOrDefault(x => x.Key.StartsWith(indexExtensionKeyPrefix)).Value;
        }

        public void SetExtension(string indexExtensionKey, IIndexExtension extension)
        {
            indexExtensions.TryAdd(indexExtensionKey, extension);
        }

        private static Document CloneDocument(Document luceneDoc)
        {
            var clonedDocument = new Document();
            foreach (AbstractField field in luceneDoc.GetFields())
            {
                var numericField = field as NumericField;
                if (numericField != null)
                {
                    var clonedNumericField = new NumericField(numericField.Name,
                                                            numericField.IsStored ? Field.Store.YES : Field.Store.NO,
                                                            numericField.IsIndexed);
                    var numericValue = numericField.NumericValue;
                    if (numericValue is int)
                    {
                        clonedNumericField.SetIntValue((int)numericValue);
                    }
                    else if (numericValue is long)
                    {
                        clonedNumericField.SetLongValue((long)numericValue);
                    }
                    else if (numericValue is double)
                    {
                        clonedNumericField.SetDoubleValue((double)numericValue);
                    }
                    else if (numericValue is float)
                    {
                        clonedNumericField.SetFloatValue((float)numericValue);
                    }
                    clonedDocument.Add(clonedNumericField);
                }
                else
                {
                    Field clonedField;
                    if (field.IsBinary)
                    {
                        clonedField = new Field(field.Name, field.GetBinaryValue(),
                                                field.IsStored ? Field.Store.YES : Field.Store.NO);
                    }
                    else if (field.StringValue != null)
                    {
                        clonedField = new Field(field.Name, field.StringValue,
                                                field.IsStored ? Field.Store.YES : Field.Store.NO,
                                                field.IsIndexed ? Field.Index.ANALYZED_NO_NORMS : Field.Index.NOT_ANALYZED_NO_NORMS,
                                                field.IsTermVectorStored ? Field.TermVector.YES : Field.TermVector.NO);
                    }
                    else
                    {
                        //probably token stream, and we can't handle fields with token streams, so we skip this.
                        continue;
                    }
                    clonedDocument.Add(clonedField);
                }
            }
            return clonedDocument;
        }

        protected void LogIndexedDocument(string key, Document luceneDoc)
        {
            if (!logIndexing.IsDebugEnabled)
                return;

            var fieldsForLogging = luceneDoc.GetFields().Select(x => new
            {
                x.Name,
                Value = x.IsBinary ? "<binary>" : x.StringValue,
                Indexed = x.IsIndexed,
                Stored = x.IsStored,
            });
            var sb = new StringBuilder();
            foreach (var fieldForLogging in fieldsForLogging)
            {
                sb.Append("\t").Append(fieldForLogging.Name)
                    .Append(" ")
                    .Append(fieldForLogging.Indexed ? "I" : "-")
                    .Append(fieldForLogging.Stored ? "S" : "-")
                    .Append(": ")
                    .Append(fieldForLogging.Value)
                    .AppendLine();
            }

            logIndexing.Debug("Indexing on {0} result in index {1} gave document: {2}", key, PublicName,
                sb.ToString());
        }

        [CLSCompliant(false)]
        public static void AssertQueryDoesNotContainFieldsThatAreNotIndexed(IndexQuery indexQuery, AbstractViewGenerator viewGenerator)
        {
            if (string.IsNullOrWhiteSpace(indexQuery.Query) == false)
            {
                HashSet<string> hashSet = SimpleQueryParser.GetFields(indexQuery);
                foreach (string field in hashSet)
                {
                    string f = field;
                    if (f.EndsWith("_Range"))
                    {
                        f = f.Substring(0, f.Length - "_Range".Length);
                    }
                    if (viewGenerator.ContainsField(f) == false &&
                        viewGenerator.ContainsField("_") == false) // the catch all field name means that we have dynamic fields names
                        throw new ArgumentException("The field '" + f + "' is not indexed, cannot query on fields that are not indexed");
                }
            }
            if (indexQuery.SortedFields != null)
            {
                foreach (SortedField field in indexQuery.SortedFields)
                {
                    string f = field.Field;
                    if (f == Constants.TemporaryScoreValue)
                        continue;
                    if (f.EndsWith("_Range"))
                    {
                        f = f.Substring(0, f.Length - "_Range".Length);
                    }
                    if (f.StartsWith(Constants.RandomFieldName) || f.StartsWith(Constants.CustomSortFieldName))
                        continue;
                    if (viewGenerator.ContainsField(f) == false && !f.StartsWith(Constants.DistanceFieldName)
                            && viewGenerator.ContainsField("_") == false) // the catch all field name means that we have dynamic fields names
                        throw new ArgumentException("The field '" + f + "' is not indexed, cannot sort on fields that are not indexed");
                }
            }
        }



        #region Nested type: IndexQueryOperation

        public class IndexQueryOperation
        {
            FastVectorHighlighter highlighter;
            FieldQuery fieldQuery;

            private readonly Stopwatch _queryParseDuration = new Stopwatch();
            private readonly IndexQuery indexQuery;
            private readonly Index parent;
            private readonly Func<IndexQueryResult, bool> shouldIncludeInResults;
            private readonly HashSet<RavenJObject> alreadySeenProjections;
            private readonly FieldsToFetch fieldsToFetch;
            private readonly HashSet<string> alreadySeenDocumentKeysInPreviousPage = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            private readonly OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers;
            private readonly List<string> reduceKeys;
            private bool hasMultipleIndexOutputs;
            private int alreadyScannedForDuplicates;

            public TimeSpan QueryParseDuration
            {
                get { return _queryParseDuration.Elapsed; }
            }
            public IndexQueryOperation(Index parent, IndexQuery indexQuery, Func<IndexQueryResult, bool> shouldIncludeInResults, FieldsToFetch fieldsToFetch, OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers, List<string> reduceKeys = null)
            {
                this.parent = parent;
                this.indexQuery = indexQuery;
                this.shouldIncludeInResults = shouldIncludeInResults;
                this.fieldsToFetch = fieldsToFetch;
                this.indexQueryTriggers = indexQueryTriggers;
                this.reduceKeys = reduceKeys;

                if (fieldsToFetch.IsDistinctQuery)
                    alreadySeenProjections = new HashSet<RavenJObject>(RavenJTokenEqualityComparer.Default);
            }

            public IEnumerable<RavenJObject> IndexEntries(Reference<int> totalResults)
            {
                var returnFullEntries = reduceKeys == null || reduceKeys.Count == 0;
                HashSet<RavenJToken> reduceValuesJson = null;
                if (returnFullEntries == false)
                {
                    reduceValuesJson = new HashSet<RavenJToken>(RavenJTokenEqualityComparer.Default);
                    foreach (var reduceKey in reduceKeys)
                    {
                        reduceValuesJson.Add(new RavenJValue(reduceKey));
                    }
                }
                parent.MarkQueried();
                using (CultureHelper.EnsureInvariantCulture())
                {
                    AssertQueryDoesNotContainFieldsThatAreNotIndexed(indexQuery, parent.viewGenerator);
                    IndexSearcher indexSearcher;
                    RavenJObject[] termsDocs;
                    using (parent.GetSearcherAndTermsDocs(out indexSearcher, out termsDocs))
                    {
                        var documentQuery = GetDocumentQuery();

                        TopDocs search = ExecuteQuery(indexSearcher, documentQuery, indexQuery.Start, indexQuery.PageSize, indexQuery);
                        totalResults.Value = search.TotalHits;

                        for (int index = indexQuery.Start; index < search.ScoreDocs.Length; index++)
                        {
                            var scoreDoc = search.ScoreDocs[index];
                            var ravenJObject = (RavenJObject)termsDocs[scoreDoc.Doc].CloneToken();
                            foreach (var prop in ravenJObject.Where(x => x.Key.EndsWith("_Range")).ToArray())
                            {
                                ravenJObject.Remove(prop.Key);
                            }

                            if (returnFullEntries)
                            {
                                yield return ravenJObject;
                                continue;
                            }
                            RavenJToken reduceKeyValue;
                            if (ravenJObject.TryGetValue(Constants.ReduceKeyFieldName, out reduceKeyValue) && reduceValuesJson.Contains(reduceKeyValue))
                            {
                                yield return ravenJObject;
                            }
                        }
                    }
                }
            }

            public IEnumerable<IndexQueryResult> Query(CancellationToken token)
            {
                if (parent.Priority.HasFlag(IndexingPriority.Error))
                    throw new IndexDisabledException("The index has been disabled due to errors");

                parent.MarkQueried();
                using (CultureHelper.EnsureInvariantCulture())
                {
                    AssertQueryDoesNotContainFieldsThatAreNotIndexed(indexQuery, parent.viewGenerator);
                    IndexSearcher indexSearcher;
                    using (parent.GetSearcher(out indexSearcher))
                    {
                        var documentQuery = GetDocumentQuery();

                        int start = indexQuery.Start;
                        int pageSize = indexQuery.PageSize;
                        int returnedResults = 0;
                        bool endOfResults;

                        int maxNumberOfIndexOutputs;

                        if (parent.MaxIndexOutputsPerDocument != null)
                        {
                            hasMultipleIndexOutputs = true;
                            maxNumberOfIndexOutputs = parent.MaxIndexOutputsPerDocument.Value;
                        }
                        else
                        {
                            maxNumberOfIndexOutputs = parent.IsMapReduce ? parent.context.Configuration.MaxMapReduceIndexOutputsPerDocument : parent.context.Configuration.MaxSimpleIndexOutputsPerDocument;

                            if (maxNumberOfIndexOutputs == -1) // configuration was set to disable output count check, probably because there exist fanout indexes
                                maxNumberOfIndexOutputs = 50;
                        }

                        var docsToGet = pageSize;
                        var position = start;

                        do
                        {
                            token.ThrowIfCancellationRequested();

                            var search = ExecuteQuery(indexSearcher, documentQuery, start, docsToGet, indexQuery);

                            indexQuery.TotalSize.Value = search.TotalHits;

                            RecordAlreadyPagedItemsInPreviousPage(start, search, indexSearcher);

                            SetupHighlighter(documentQuery);

                            for (; position < search.ScoreDocs.Length && pageSize > 0; position++)
                            {
                                token.ThrowIfCancellationRequested();

                                var scoreDoc = search.ScoreDocs[position];
                                var document = indexSearcher.Doc(scoreDoc.Doc);
                                var indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, scoreDoc);
                                if (indexQueryResult.Key == null && !string.IsNullOrEmpty(indexQuery.HighlighterKeyName))
                                {
                                    indexQueryResult.HighlighterKey = document.Get(indexQuery.HighlighterKeyName);
                                }

                                if (ShouldIncludeInResults(indexQueryResult) == false)
                                {
                                    indexQuery.SkippedResults.Value++;
                                    continue;
                                }

                                AddHighlighterResults(indexSearcher, scoreDoc, indexQueryResult);

                                AddQueryExplanation(documentQuery, indexSearcher, scoreDoc, indexQueryResult);

                                returnedResults++;

                                yield return indexQueryResult;
                                if (returnedResults == pageSize)
                                    yield break;
                            }

                            if (hasMultipleIndexOutputs)
                                docsToGet += (pageSize - returnedResults) * maxNumberOfIndexOutputs;
                            else
                                docsToGet += (pageSize - returnedResults);

                            endOfResults = search.TotalHits == search.ScoreDocs.Length;

                        } while (returnedResults < pageSize && endOfResults == false);
                    }
                }
            }

            private void RecordAlreadyPagedItemsInPreviousPage(int start, TopDocs search, IndexSearcher indexSearcher)
            {
                if (start == 0)
                    return;

                if (indexQuery.SkipDuplicateChecking)
                    return;

                // we are paging, we need to check that we don't have duplicates in the previous pages
                // see here for details: http://groups.google.com/group/ravendb/browse_frm/thread/d71c44aa9e2a7c6e

                if (parent.IsMapReduce == false && fieldsToFetch.IsProjection == false && search.ScoreDocs.Length >= start)
                {
                    if (IsSortingQuery(indexQuery))
                    {
                        // we need to scan all records from the beginning to requested 'start' position
                        for (int i = 0; i < start && i < search.ScoreDocs.Length; i++)
                        {
                            var scoreDoc = search.ScoreDocs[i];
                            var document = indexSearcher.Doc(scoreDoc.Doc);
                            var alreadyPagedKey = document.Get(Constants.DocumentIdFieldName);

                            alreadySeenDocumentKeysInPreviousPage.Add(alreadyPagedKey);
                            hasMultipleIndexOutputs = true;
                        }
                    }
                    else
                    {
                        // that's not a sorted query so we need just to ensure that we won't return the last item of the previous page
                        var scoreDoc = search.ScoreDocs[start - 1];
                        var document = indexSearcher.Doc(scoreDoc.Doc);
                        var alreadyPagedKey = document.Get(Constants.DocumentIdFieldName);

                        alreadySeenDocumentKeysInPreviousPage.Add(alreadyPagedKey);
                        hasMultipleIndexOutputs = true;
                    }
                }

                if (fieldsToFetch.IsDistinctQuery)
                {
                    for (; alreadySeenProjections.Count < start && alreadyScannedForDuplicates < search.ScoreDocs.Length; alreadyScannedForDuplicates++)
                    {
                        var scoreDoc = search.ScoreDocs[alreadyScannedForDuplicates];
                        var document = indexSearcher.Doc(scoreDoc.Doc);
                        var indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, scoreDoc);

                        if (indexQueryResult.Projection.Count > 0) // we don't consider empty projections to be relevant for distinct operations
                        {
                            alreadySeenProjections.Add(indexQueryResult.Projection);
                        }
                    }
                }
            }

            private bool IsSortingQuery(IndexQuery query)
            {
                return query.SortedFields != null && query.SortedFields.Length > 0;
            }

            private void AddHighlighterResults(IndexSearcher indexSearcher, ScoreDoc scoreDoc, IndexQueryResult indexQueryResult)
            {
                if (highlighter == null)
                    return;

                var highlightings =
                    (from highlightedField in this.indexQuery.HighlightedFields
                     select new
                     {
                         highlightedField.Field,
                         highlightedField.FragmentsField,
                         Fragments = highlighter.GetBestFragments(
                             fieldQuery,
                             indexSearcher.IndexReader,
                             scoreDoc.Doc,
                             highlightedField.Field,
                             highlightedField.FragmentLength,
                             highlightedField.FragmentCount)
                     }
                         into fieldHighlitings
                         where fieldHighlitings.Fragments != null &&
                               fieldHighlitings.Fragments.Length > 0
                         select fieldHighlitings).ToList();

                if (indexQueryResult.Projection != null)
                {
                    foreach (var highlighting in highlightings)
                    {
                        if (!string.IsNullOrEmpty(highlighting.FragmentsField))
                        {
                            indexQueryResult.Projection[highlighting.FragmentsField] = new RavenJArray(highlighting.Fragments);
                        }
                    }
                }
                indexQueryResult.Highligtings = highlightings.ToDictionary(x => x.Field, x => x.Fragments);
            }

            private void SetupHighlighter(Query documentQuery)
            {
                if (indexQuery.HighlightedFields != null && indexQuery.HighlightedFields.Length > 0)
                {
                    highlighter = new FastVectorHighlighter(
                        FastVectorHighlighter.DEFAULT_PHRASE_HIGHLIGHT,
                        FastVectorHighlighter.DEFAULT_FIELD_MATCH,
                        new SimpleFragListBuilder(),
                        new SimpleFragmentsBuilder(
                            indexQuery.HighlighterPreTags != null && indexQuery.HighlighterPreTags.Any()
                                ? indexQuery.HighlighterPreTags
                                : BaseFragmentsBuilder.COLORED_PRE_TAGS,
                            indexQuery.HighlighterPostTags != null && indexQuery.HighlighterPostTags.Any()
                                ? indexQuery.HighlighterPostTags
                                : BaseFragmentsBuilder.COLORED_POST_TAGS));

                    fieldQuery = highlighter.GetFieldQuery(documentQuery);
                }
            }

            private void AddQueryExplanation(Query documentQuery, IndexSearcher indexSearcher, ScoreDoc scoreDoc, IndexQueryResult indexQueryResult)
            {
                if (indexQuery.ExplainScores == false)
                    return;

                var explanation = indexSearcher.Explain(documentQuery, scoreDoc.Doc);

                indexQueryResult.ScoreExplanation = explanation.ToString();
            }

            private Query ApplyIndexTriggers(Query documentQuery)
            {
                documentQuery = indexQueryTriggers.Aggregate(documentQuery,
                                                           (current, indexQueryTrigger) =>
                                                           indexQueryTrigger.Value.ProcessQuery(parent.PublicName, current, indexQuery));
                return documentQuery;
            }

            public IEnumerable<IndexQueryResult> IntersectionQuery(CancellationToken token)
            {
                using (CultureHelper.EnsureInvariantCulture())
                {
                    AssertQueryDoesNotContainFieldsThatAreNotIndexed(indexQuery, parent.viewGenerator);
                    IndexSearcher indexSearcher;
                    using (parent.GetSearcher(out indexSearcher))
                    {
                        var subQueries = indexQuery.Query.Split(new[] { Constants.IntersectSeparator }, StringSplitOptions.RemoveEmptyEntries);
                        if (subQueries.Length <= 1)
                            throw new InvalidOperationException("Invalid INTERSECT query, must have multiple intersect clauses.");

                        //Not sure how to select the page size here??? The problem is that only docs in this search can be part 
                        //of the final result because we're doing an intersection query (but we might exclude some of them)
                        int pageSizeBestGuess = (indexQuery.Start + indexQuery.PageSize) * 2;
                        int intersectMatches = 0, skippedResultsInCurrentLoop = 0;
                        int previousBaseQueryMatches = 0, currentBaseQueryMatches = 0;

                        var firstSubDocumentQuery = GetDocumentQuery(subQueries[0], indexQuery);

                        //Do the first sub-query in the normal way, so that sorting, filtering etc is accounted for
                        var search = ExecuteQuery(indexSearcher, firstSubDocumentQuery, 0, pageSizeBestGuess, indexQuery);
                        currentBaseQueryMatches = search.ScoreDocs.Length;
                        var intersectionCollector = new IntersectionCollector(indexSearcher, search.ScoreDocs);

                        do
                        {
                            token.ThrowIfCancellationRequested();
                            if (skippedResultsInCurrentLoop > 0)
                            {
                                // We get here because out first attempt didn't get enough docs (after INTERSECTION was calculated)
                                pageSizeBestGuess = pageSizeBestGuess * 2;

                                search = ExecuteQuery(indexSearcher, firstSubDocumentQuery, 0, pageSizeBestGuess, indexQuery);
                                previousBaseQueryMatches = currentBaseQueryMatches;
                                currentBaseQueryMatches = search.ScoreDocs.Length;
                                intersectionCollector = new IntersectionCollector(indexSearcher, search.ScoreDocs);
                            }

                            for (int i = 1; i < subQueries.Length; i++)
                            {
                                var luceneSubQuery = GetDocumentQuery(subQueries[i], indexQuery);
                                indexSearcher.Search(luceneSubQuery, null, intersectionCollector);
                            }

                            var currentIntersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
                            intersectMatches = currentIntersectResults.Count;
                            skippedResultsInCurrentLoop = pageSizeBestGuess - intersectMatches;
                        } while (intersectMatches < indexQuery.PageSize && //stop if we've got enough results to satisfy the pageSize
                                 currentBaseQueryMatches < search.TotalHits && //stop if increasing the page size wouldn't make any difference
                                 previousBaseQueryMatches < currentBaseQueryMatches); //stop if increasing the page size didn't result in any more "base query" results

                        var intersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
                        //It's hard to know what to do here, the TotalHits from the base search isn't really the TotalSize, 
                        //because it's before the INTERSECTION has been applied, so only some of those results make it out.
                        //Trying to give an accurate answer is going to be too costly, so we aren't going to try.
                        indexQuery.TotalSize.Value = search.TotalHits;
                        indexQuery.SkippedResults.Value = skippedResultsInCurrentLoop;

                        //Using the final set of results in the intersectionCollector
                        int returnedResults = 0;
                        for (int i = indexQuery.Start; i < intersectResults.Count && (i - indexQuery.Start) < pageSizeBestGuess; i++)
                        {
                            Document document = indexSearcher.Doc(intersectResults[i].LuceneId);
                            IndexQueryResult indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, search.ScoreDocs[i]);

                            if (ShouldIncludeInResults(indexQueryResult) == false)
                            {
                                indexQuery.SkippedResults.Value++;
                                skippedResultsInCurrentLoop++;
                                continue;
                            }

                            returnedResults++;
                            yield return indexQueryResult;
                            if (returnedResults == indexQuery.PageSize)
                                yield break;
                        }
                    }
                }
            }
            private bool ShouldIncludeInResults(IndexQueryResult indexQueryResult)
            {
                if (shouldIncludeInResults(indexQueryResult) == false)
                    return false;

                if (alreadySeenDocumentKeysInPreviousPage.Contains(indexQueryResult.Key))
                {
                    hasMultipleIndexOutputs = true;
                    return false;
                }

                if (fieldsToFetch.IsDistinctQuery && alreadySeenProjections.Add(indexQueryResult.Projection) == false)
                    return false;

                return true;
            }

            public Query GetDocumentQuery()
            {
                var q = GetDocumentQuery(indexQuery.Query, indexQuery);
                var spatialIndexQuery = indexQuery as SpatialIndexQuery;
                if (spatialIndexQuery != null)
                {
                    var spatialField = parent.viewGenerator.GetSpatialField(spatialIndexQuery.SpatialFieldName);
                    var dq = spatialField.MakeQuery(q, spatialField.GetStrategy(), spatialIndexQuery);
                    if (q is MatchAllDocsQuery) return dq;

                    var bq = new BooleanQuery { { q, Occur.MUST }, { dq, Occur.MUST } };
                    return bq;
                }
                return q;
            }

            private Query GetDocumentQuery(string query, IndexQuery indexQuery)
            {
                _queryParseDuration.Start();
                Query documentQuery;
                if (String.IsNullOrEmpty(query))
                {
                    if (logQuerying.IsDebugEnabled)
                        logQuerying.Debug("Issuing query on index {0} for all documents", parent.PublicName);
                    documentQuery = new MatchAllDocsQuery();
                }
                else
                {
                    if (logQuerying.IsDebugEnabled)
                        logQuerying.Debug("Issuing query on index {0} for: {1}", parent.PublicName, query);
                    var toDispose = new List<Action>();
                    RavenPerFieldAnalyzerWrapper searchAnalyzer = null;
                    try
                    {
                        searchAnalyzer = parent.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose, true);
                        searchAnalyzer = parent.AnalyzerGenerators.Aggregate(searchAnalyzer, (currentAnalyzer, generator) =>
                        {
                            Analyzer newAnalyzer = generator.GenerateAnalyzerForQuerying(parent.PublicName, indexQuery.Query, currentAnalyzer);
                            if (newAnalyzer != currentAnalyzer)
                            {
                                DisposeAnalyzerAndFriends(toDispose, currentAnalyzer);
                            }
                            return parent.CreateAnalyzer(newAnalyzer, toDispose, true);
                        });
                        documentQuery = QueryBuilder.BuildQuery(query, indexQuery, searchAnalyzer);
                    }
                    finally
                    {
                        DisposeAnalyzerAndFriends(toDispose, searchAnalyzer);
                    }
                }
                var afterTriggers = ApplyIndexTriggers(documentQuery);
                _queryParseDuration.Stop();
                return afterTriggers;
            }

            private static void DisposeAnalyzerAndFriends(List<Action> toDispose, RavenPerFieldAnalyzerWrapper analyzer)
            {
                if (analyzer != null)
                    analyzer.Close();
                foreach (Action dispose in toDispose)
                {
                    dispose();
                }
                toDispose.Clear();
            }

            private TopDocs ExecuteQuery(IndexSearcher indexSearcher, Query documentQuery, int start, int pageSize,
                                        IndexQuery indexQuery)
            {
                var sort = indexQuery.GetSort(parent.indexDefinition, parent.viewGenerator);

                if (pageSize == Int32.MaxValue && sort == null) // we want all docs, no sorting required
                {
                    var gatherAllCollector = new GatherAllCollector();
                    indexSearcher.Search(documentQuery, gatherAllCollector);
                    return gatherAllCollector.ToTopDocs();
                }
                int absFullPage = Math.Abs(pageSize + start); // need to protect against ridiculously high values of pageSize + start that overflow
                var minPageSize = Math.Max(absFullPage, 1);

                // NOTE: We get Start + Pagesize results back so we have something to page on
                if (sort != null)
                {
                    try
                    {
                        //indexSearcher.SetDefaultFieldSortScoring (sort.GetSort().Contains(SortField.FIELD_SCORE), false);
                        indexSearcher.SetDefaultFieldSortScoring(true, false);
                        var ret = indexSearcher.Search(documentQuery, null, minPageSize, sort);
                        return ret;
                    }
                    finally
                    {
                        indexSearcher.SetDefaultFieldSortScoring(false, false);
                    }
                }
                return indexSearcher.Search(documentQuery, null, minPageSize);
            }
        }

        #endregion

        public IndexingPerformanceStats[] GetIndexingPerformance()
        {
            return currentlyIndexing.Values.Concat(indexingPerformanceStats).ToArray();
        }

        public IndexingPerformanceStats[] GetCurrentIndexingPerformance()
        {
            return currentlyIndexing.Values.ToArray();
        }

        public void Backup(string backupDirectory, string path, string incrementalTag, Action<string, string, BackupStatus.BackupMessageSeverity> notifyCallback)
        {
            if (directory is RAMDirectory)
            {
                //if the index is memory-only, force writing index data to disk
                Write((writer, analyzer, stats) =>
                {
                    ForceWriteToDisk();
                    return new IndexedItemsInfo(GetLastEtagFromStats()) { ChangedDocs = 1 };
                });
            }

            bool hasSnapshot = false;
            bool throwOnFinallyException = true;
            try
            {
                var existingFiles = new HashSet<string>();
                if (incrementalTag != null)
                    backupDirectory = Path.Combine(backupDirectory, incrementalTag);

                var allFilesPath = Path.Combine(backupDirectory, indexId + ".all-existing-index-files");
                var saveToFolder = Path.Combine(backupDirectory, "Indexes", indexId.ToString());
                System.IO.Directory.CreateDirectory(saveToFolder);
                if (File.Exists(allFilesPath))
                {
                    foreach (var file in File.ReadLines(allFilesPath))
                    {
                        existingFiles.Add(file);
                    }
                }

                var neededFilePath = Path.Combine(saveToFolder, "index-files.required-for-index-restore");
                using (var allFilesWriter = File.Exists(allFilesPath) ? File.AppendText(allFilesPath) : File.CreateText(allFilesPath))
                using (var neededFilesWriter = File.CreateText(neededFilePath))
                {
                    try
                    {
                        // this is called for the side effect of creating the snapshotter and the writer
                        // we explicitly handle the backup outside of the write, to allow concurrent indexing
                        Write((writer, analyzer, stats) =>
                        {
                            // however, we copy the current segments.gen & index.version to make 
                            // sure that we get the _at the time_ of the write. 
                            foreach (var fileName in new[] { "segments.gen", IndexStorage.IndexVersionFileName(indexDefinition) })
                            {
                                var fullPath = Path.Combine(path, indexId.ToString(), fileName);
                                File.Copy(fullPath, Path.Combine(saveToFolder, fileName));
                                allFilesWriter.WriteLine(fileName);
                                neededFilesWriter.WriteLine(fileName);
                            }
                            return new IndexedItemsInfo(null);
                        });
                    }
                    catch (CorruptIndexException e)
                    {
                        var failureMessage = "Could not backup index " + PublicName + " because it is corrupted. Skipping the index, will force index reset on restore";
                        LogErrorAndNotifyStudio(notifyCallback, failureMessage, e);
                        neededFilesWriter.Dispose();
                        TryDelete(neededFilePath);
                        return;
                    }
                    IndexCommit commit;
                    try
                    {
                        commit = snapshotter.Snapshot();
                        hasSnapshot = true;
                    }
                    catch (Exception)
                    {
                        hasSnapshot = false;
                        commit = null;
                    }
                    if (hasSnapshot)
                    {
                        foreach (var fileName in commit.FileNames)
                        {
                            var fullPath = Path.Combine(path, indexId.ToString(), fileName);

                            if (".lock".Equals(Path.GetExtension(fullPath), StringComparison.InvariantCultureIgnoreCase))
                                continue;

                            if (File.Exists(fullPath) == false)
                                continue;

                            if (existingFiles.Contains(fileName) == false)
                            {
                                var destFileName = Path.Combine(saveToFolder, fileName);
                                try
                                {
                                    File.Copy(fullPath, destFileName);
                                }
                                catch (Exception e)
                                {
                                    var failureMessage = "Could not backup index " + PublicName + " because failed to copy file : " + fullPath +
                                        ". Skipping the index, will force index reset on restore";
                                    LogErrorAndNotifyStudio(notifyCallback, failureMessage, e);
                                    neededFilesWriter.Dispose();
                                    TryDelete(neededFilePath);
                                    return;

                                }
                                allFilesWriter.WriteLine(fileName);
                            }
                            neededFilesWriter.WriteLine(fileName);
                        }
                    }
                    allFilesWriter.Flush();
                    neededFilesWriter.Flush();
                }
            }
            catch (Exception e)
            {
                var failureMessage = "Could not backup index " + PublicName +
                                     " because an unexpected exception was thrown. Skipping the index, will force index reset on restore";
                LogErrorAndNotifyStudio(notifyCallback, failureMessage, e);
                try
                {
                    File.WriteAllText(Path.Combine(backupDirectory, String.Format("{0}.backup_failed", indexId)), e.ToString());
                }
                catch (Exception fe)
                {
                    failureMessage = "failed to create fail index file for index " + PublicName +
                                         " because an unexpected exception was thrown. This error may prevent auto reseting of the index on restore.";
                    LogErrorAndNotifyStudio(notifyCallback, failureMessage, fe);
                    throwOnFinallyException = false;
                    throw;
                }
            }
            finally
            {
                if (snapshotter != null && hasSnapshot)
                {
                    try
                    {
                        snapshotter.Release();
                    }
                    catch (Exception e)
                    {
                        var failureMessage = "Failed to release snapshotter while backing-up index " + PublicName;
                        LogErrorAndNotifyStudio(notifyCallback, failureMessage, e);
                        if (throwOnFinallyException) throw;
                    }
                }
            }
        }

        private static void LogErrorAndNotifyStudio(Action<string, string, BackupStatus.BackupMessageSeverity> notifyCallback, string failureMessage, Exception e)
        {
            logIndexing.WarnException(failureMessage, e);
            if (notifyCallback != null)
                notifyCallback(failureMessage, null, BackupStatus.BackupMessageSeverity.Error);
        }

        public Etag GetLastEtagFromStats()
        {
            if (context.IndexStorage == null) // startup
                return Etag.Empty;
            return context.IndexStorage.GetLastEtagForIndex(this);
        }

        private static void TryDelete(string neededFilePath)
        {
            try
            {
                File.Delete(neededFilePath);
            }
            catch (Exception)
            {
            }
        }

        protected void UpdateDocumentReferences(IStorageActionsAccessor actions,
            ConcurrentQueue<IDictionary<string, HashSet<string>>> allReferencedDocs,
            ConcurrentQueue<IDictionary<string, Etag>> missingReferencedDocs)
        {

            IDictionary<string, HashSet<string>> merged = new Dictionary<string, HashSet<string>>(StringComparer.InvariantCultureIgnoreCase);
            IDictionary<string, HashSet<string>> result;
            while (allReferencedDocs.TryDequeue(out result))
            {
                foreach (var kvp in result)
                {
                    HashSet<string> set;
                    if (merged.TryGetValue(kvp.Key, out set))
                    {
                        logIndexing.Debug("Merging references for key = {0}, references = {1}", kvp.Key, String.Join(",", set));
                        set.UnionWith(kvp.Value);
                    }
                    else
                    {
                        merged.Add(kvp.Key, kvp.Value);
                    }
                }
            }

            foreach (var referencedDocument in merged)
            {
                actions.Indexing.UpdateDocumentReferences(indexId, referencedDocument.Key, referencedDocument.Value);
                actions.General.MaybePulseTransaction();
            }

            // so we will get IsStale properly
            var task = new TouchReferenceDocumentIfChangedTask(indexId);

            IDictionary<string, Etag> docs;
            while (missingReferencedDocs.TryDequeue(out docs))
            {
                foreach (var doc in docs)
                {
                    task.UpdateReferenceToCheck(doc);
                }

                if (task.NumberOfKeys > 0)
                    logIndexing.Debug("Scheduled to touch documents: {0}", string.Join(";", task.GetReferencesForDebug()));
            }

            if (task.NumberOfKeys == 0)
                return;

            actions.Tasks.AddTask(task, SystemTime.UtcNow);
        }

        public void ForceWriteToDisk()
        {
            forceWriteToDisk = true;
        }

        protected bool EnsureValidNumberOfOutputsForDocument(string sourceDocumentId, int numberOfAlreadyProducedOutputs)
        {
            if (indexDefinition.MaxIndexOutputsPerDocument != null)
            {
                // user has specifically configured this value, but we don't trust it.
           
                var actualIndexOutput = maxActualIndexOutput;
                if (actualIndexOutput < numberOfAlreadyProducedOutputs)
                {
                    // okay, now let verify that this is indeed the case, in thread safe manner,
                    // this way, most of the time we don't need volatile reads, and other sync operations
                    // in the code ensure we don't have too stale a view on the data (beside, stale view have
                    // to mean a smaller number, which we then verify).
                    actualIndexOutput = Thread.VolatileRead(ref maxActualIndexOutput);
                    while (actualIndexOutput < numberOfAlreadyProducedOutputs)
                    {
                        // if it changed, we don't care, it is just another max, and another thread probably
                        // set it for us, so we only retry if this is still smaller
                        actualIndexOutput = Interlocked.CompareExchange(
                            ref maxActualIndexOutput, 
                            numberOfAlreadyProducedOutputs,
                            actualIndexOutput);
                    }
                }
            }

            var maxNumberOfIndexOutputs = indexDefinition.MaxIndexOutputsPerDocument ??
                                        (IsMapReduce ? context.Configuration.MaxMapReduceIndexOutputsPerDocument : context.Configuration.MaxSimpleIndexOutputsPerDocument);

            if (maxNumberOfIndexOutputs == -1)
                return true;

            if (numberOfAlreadyProducedOutputs <= maxNumberOfIndexOutputs)
                return true;

            var msg = string.Format("Index '{0}' has already produced {1} map results for a source document '{2}', while the allowed max number of outputs is {3} per one document. " +
                                    "Please verify this index definition and consider a re-design of your entities or index.",
                PublicName, numberOfAlreadyProducedOutputs, sourceDocumentId, maxNumberOfIndexOutputs);
            logIndexing.Warn(msg);
            context.AddError(indexId, PublicName, sourceDocumentId, msg);

            return false;
        }

        public void IncrementWriteErrors(Exception e)
        {
            if (e is SystemException) // Don't count transient errors
                return;

            var errors = Interlocked.Increment(ref writeErrors);

            if (errors < WriteErrorsLimit || Priority == IndexingPriority.Error)
                return;

            var msg = string.Format("Index '{0}' failed {1} times to write data to a disk. The index priority was set to Error.", PublicName, WriteErrorsLimit);
            var title = string.Format("Index '{0}' marked as errored due to write errors", PublicName);
            AddIndexError(msg, title);
        }

        private void AddIndexError(string msg, string title)
        {
            context.Database.TransactionalStorage.Batch(accessor => accessor.Indexing.SetIndexPriority(indexId, IndexingPriority.Error));
            Priority = IndexingPriority.Error;

            context.Database.Notifications.RaiseNotifications(new IndexChangeNotification
            {
                Name = PublicName,
                Type = IndexChangeTypes.IndexMarkedAsErrored
            });

            logIndexing.Warn(msg);

            context.AddError(indexId, PublicName, null, msg);

            context.Database.AddAlert(new Alert
            {
                AlertLevel = AlertLevel.Error,
                CreatedAt = SystemTime.UtcNow,
                Message = msg,
                Title = title,
                UniqueKey = string.Format("Index '{0}' errored, dbid: {1}", PublicName, context.Database.TransactionalStorage.Id),
            });
        }

        public void AddIndexCorruptError(Exception e)
        {
            var msg = string.Format("Index '{0}' is corrupted. The index priority was set to Error. Exception: {1}", PublicName, e);
            var title = string.Format("Index '{0}' marked as errored due to corruption", PublicName);
            AddIndexError(msg, title);
        }

        public void AddIndexFailedFlushError(Exception e)
        {
            var msg = string.Format("Failed to flush index '{0}'. The index priority was set to Error. Exception: {1}", PublicName, e);
            var title = string.Format("Index '{0}' marked as errored due to failure to flush index", PublicName);
            AddIndexError(msg, title);
        }

        private void ResetWriteErrors()
        {
            Interlocked.Exchange(ref writeErrors, 0);
        }

        internal class IndexByIdEqualityComparer : IEqualityComparer<Index>
        {
            public bool Equals(Index x, Index y)
            {
                return x.IndexId == y.IndexId;
            }

            public int GetHashCode(Index obj)
            {
                return obj.IndexId.GetHashCode();
            }
        }

        public void HandleLowMemory()
        {
            bool tryEnter = false;
            try
            {
                tryEnter = Monitor.TryEnter(writeLock);

                if (tryEnter == false)
                    return;

                try
                {
                    // if in memory, flush to disk
                    if (indexWriter != null)
                    {
                        ForceWriteToDisk();
                        WriteInMemoryIndexToDiskIfNecessary(GetLastEtagFromStats());
                    }
                }
                catch (Exception e)
                {
                    logIndexing.ErrorException("Error while writing in memory index to disk.", e);
                }
                RecreateSearcher();
            }
            finally
            {
                if(tryEnter)
                Monitor.Exit(writeLock);
            }
        }

        public void SoftMemoryRelease()
        {
            
        }

        public LowMemoryHandlerStatistics GetStats()
        {
            var writerEstimator = new RamUsageEstimator(false);
            return new LowMemoryHandlerStatistics()
            {
                Name = "Index",
                DatabaseName = this.context.DatabaseName,
                Metadata = new
                {
                    IndexName = this.PublicName
                },
                EstimatedUsedMemory = writerEstimator.EstimateRamUsage(indexWriter)
            };
        }
    }
}
