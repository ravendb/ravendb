//-----------------------------------------------------------------------
// <copyright file="MapReduceIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Data;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Spatial4n.Core.Exceptions;
using Sparrow.Collections;

namespace Raven.Database.Indexing
{
    internal class MapReduceIndex : Index
    {
        readonly JsonSerializer jsonSerializer;

        private static readonly JsonConverterCollection MapReduceConverters;

        static MapReduceIndex()
        {
            MapReduceConverters = new JsonConverterCollection(Default.Converters)
            {
                new IgnoreFieldable()
            };

            MapReduceConverters.Freeze();
        }

        private class IgnoreFieldable : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                writer.WriteValue("IgnoredLuceueField");
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                return null;
            }

            public override bool CanConvert(Type objectType)
            {
                return typeof(IFieldable).IsAssignableFrom(objectType) ||
                       typeof(IEnumerable<AbstractField>).IsAssignableFrom(objectType);
            }
        }

        public MapReduceIndex(Directory directory, int id, IndexDefinition indexDefinition,
                              AbstractViewGenerator viewGenerator, WorkContext context)
            : base(directory, id, indexDefinition, viewGenerator, context)
        {
            jsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
            jsonSerializer.Converters = MapReduceConverters;
        }

        public override bool IsMapReduce
        {
            get { return true; }
        }

        public override IndexingPerformanceStats IndexDocuments(AbstractViewGenerator viewGenerator, IndexingBatch batch, IStorageActionsAccessor actions, DateTime minimumTimestamp, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            var count = 0;
            var sourceCount = batch.Docs.Count;
            var deleted = new Dictionary<ReduceKeyAndBucket, int>();
            var performance = RecordCurrentBatch("Current Map", "Map", batch.Docs.Count);
            var performanceStats = new List<BasePerformanceStats>();

            var usedStorageAccessors = new ConcurrentSet<IStorageActionsAccessor>();

            if (usedStorageAccessors.TryAdd(actions))
            {
                var storageCommitDuration = new Stopwatch();

                actions.BeforeStorageCommit += storageCommitDuration.Start;

                actions.AfterStorageCommit += () =>
                {
                    storageCommitDuration.Stop();

                    performanceStats.Add(PerformanceStats.From(IndexingOperation.StorageCommit, storageCommitDuration.ElapsedMilliseconds));
                };
            }

            List<dynamic> documentsWrapped;

            if (actions.MapReduce.HasMappedResultsForIndex(indexId) == false)
            {
                //new index
                documentsWrapped = batch.Docs.Where(x => x is FilteredDocument == false).ToList();
            }
            else
            {
                var deleteMappedResultsDuration = new Stopwatch();
                documentsWrapped = batch.Docs.Select(doc =>
                {
                    token.ThrowIfCancellationRequested();

                    var documentId = doc.__document_id;

                    using (StopwatchScope.For(deleteMappedResultsDuration))
                    {
                        actions.MapReduce.DeleteMappedResultsForDocumentId((string)documentId, indexId, deleted);
                    }

                    return doc;
                })
                .Where(x => x is FilteredDocument == false)
                .ToList();

                performanceStats.Add(new PerformanceStats
                {
                    Name = IndexingOperation.Map_DeleteMappedResults,
                    DurationMs = deleteMappedResultsDuration.ElapsedMilliseconds,
                });
            }

            var allReferencedDocs = new ConcurrentQueue<IDictionary<string, HashSet<string>>>();
            var allReferenceEtags = new ConcurrentQueue<IDictionary<string, Etag>>();
            var allState = new ConcurrentQueue<Tuple<HashSet<ReduceKeyAndBucket>, IndexingWorkStats, Dictionary<string, int>>>();

            var parallelOperations = new ConcurrentQueue<ParallelBatchStats>();

            var parallelProcessingStart = SystemTime.UtcNow;

            context.Database.MappingThreadPool.ExecuteBatch(documentsWrapped, (IEnumerator<dynamic> partition) =>
            {
                token.ThrowIfCancellationRequested();
                var parallelStats = new ParallelBatchStats
                {
                    StartDelay = (long)(SystemTime.UtcNow - parallelProcessingStart).TotalMilliseconds
                };

                var localStats = new IndexingWorkStats();
                var localChanges = new HashSet<ReduceKeyAndBucket>();
                var statsPerKey = new Dictionary<string, int>();

                var linqExecutionDuration = new Stopwatch();
                var reduceInMapLinqExecutionDuration = new Stopwatch();
                var putMappedResultsDuration = new Stopwatch();
                var convertToRavenJObjectDuration = new Stopwatch();

                allState.Enqueue(Tuple.Create(localChanges, localStats, statsPerKey));

                using (CurrentIndexingScope.Current = new CurrentIndexingScope(context.Database, PublicName))
                {
                    // we are writing to the transactional store from multiple threads here, and in a streaming fashion
                    // should result in less memory and better perf
                    context.TransactionalStorage.Batch(accessor =>
                    {
                        if (usedStorageAccessors.TryAdd(accessor))
                        {
                            var storageCommitDuration = new Stopwatch();

                            accessor.BeforeStorageCommit += storageCommitDuration.Start;

                            accessor.AfterStorageCommit += () =>
                            {
                                storageCommitDuration.Stop();

                                parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.StorageCommit, storageCommitDuration.ElapsedMilliseconds));
                            };
                        }

                        var mapResults = RobustEnumerationIndex(partition, viewGenerator.MapDefinitions, localStats, linqExecutionDuration);
                        var currentDocumentResults = new List<object>();
                        string currentKey = null;
                        bool skipDocument = false;

                        foreach (var currentDoc in mapResults)
                        {
                            token.ThrowIfCancellationRequested();

                            var documentId = GetDocumentId(currentDoc);
                            if (documentId != currentKey)
                            {
                                count += ProcessBatch(viewGenerator, currentDocumentResults, currentKey, localChanges, accessor, statsPerKey, reduceInMapLinqExecutionDuration, putMappedResultsDuration, convertToRavenJObjectDuration);

                                currentDocumentResults.Clear();
                                currentKey = documentId;
                            }
                            else if (skipDocument)
                            {
                                continue;
                            }

                            RavenJObject currentDocJObject;
                            using (StopwatchScope.For(convertToRavenJObjectDuration))
                            {
                                currentDocJObject = RavenJObject.FromObject(currentDoc, jsonSerializer);
                            }

                            currentDocumentResults.Add(new DynamicJsonObject(currentDocJObject));

                            if (EnsureValidNumberOfOutputsForDocument(documentId, currentDocumentResults.Count) == false)
                            {
                                skipDocument = true;
                                currentDocumentResults.Clear();
                                continue;
                            }

                            Interlocked.Increment(ref localStats.IndexingSuccesses);
                        }
                        count += ProcessBatch(viewGenerator, currentDocumentResults, currentKey, localChanges, accessor, statsPerKey, reduceInMapLinqExecutionDuration, putMappedResultsDuration, convertToRavenJObjectDuration);

                        parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.LoadDocument, CurrentIndexingScope.Current.LoadDocumentDuration.ElapsedMilliseconds));
                        parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Linq_MapExecution, linqExecutionDuration.ElapsedMilliseconds));
                        parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Linq_ReduceLinqExecution, reduceInMapLinqExecutionDuration.ElapsedMilliseconds));
                        parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Map_PutMappedResults, putMappedResultsDuration.ElapsedMilliseconds));
                        parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Map_ConvertToRavenJObject, convertToRavenJObjectDuration.ElapsedMilliseconds));

                        parallelOperations.Enqueue(parallelStats);
                    });

                    allReferenceEtags.Enqueue(CurrentIndexingScope.Current.ReferencesEtags);
                    allReferencedDocs.Enqueue(CurrentIndexingScope.Current.ReferencedDocuments);
                }
            }, description: $"Reducing index {PublicName} up to etag {batch.HighestEtagBeforeFiltering}, for {documentsWrapped.Count} documents");

            performanceStats.Add(new ParallelPerformanceStats
            {
                NumberOfThreads = parallelOperations.Count,
                DurationMs = (long)(SystemTime.UtcNow - parallelProcessingStart).TotalMilliseconds,
                BatchedOperations = parallelOperations.ToList()
            });

            var updateDocumentReferencesDuration = new Stopwatch();
            using (StopwatchScope.For(updateDocumentReferencesDuration))
            {
                UpdateDocumentReferences(actions, allReferencedDocs, allReferenceEtags);
            }
            performanceStats.Add(PerformanceStats.From(IndexingOperation.UpdateDocumentReferences, updateDocumentReferencesDuration.ElapsedMilliseconds));

            var changed = allState.SelectMany(x => x.Item1).Concat(deleted.Keys)
                    .Distinct()
                    .ToList();

            var stats = new IndexingWorkStats(allState.Select(x => x.Item2));
            var reduceKeyStats = allState.SelectMany(x => x.Item3)
                                         .GroupBy(x => x.Key)
                                         .Select(g => new { g.Key, Count = g.Sum(x => x.Value) })
                                         .ToList();

            var reduceKeyToCount = new ConcurrentDictionary<string, int>();
            foreach (var singleDeleted in deleted)
            {
                var reduceKey = singleDeleted.Key.ReduceKey;
                reduceKeyToCount[reduceKey] = reduceKeyToCount.GetOrDefault(reduceKey) + singleDeleted.Value;
            }

            context.Database.MappingThreadPool.ExecuteBatch(reduceKeyStats, enumerator =>
                context.TransactionalStorage.Batch(accessor =>
                {
                    while (enumerator.MoveNext())
                    {
                        var reduceKeyStat = enumerator.Current;
                        var value = 0;
                        reduceKeyToCount.TryRemove(reduceKeyStat.Key, out value);

                        var changeValue = reduceKeyStat.Count - value;
                        if (changeValue == 0)
                        {
                            // nothing to change
                            continue;
                        }

                        accessor.MapReduce.IncrementReduceKeyCounter(indexId, reduceKeyStat.Key, changeValue);
                    }
                }), 
                description: $"Incrementing reducing key counter fo index {PublicName} for operation " +
                             $"from etag {GetLastEtagFromStats()} to etag {batch.HighestEtagBeforeFiltering}");

            foreach (var keyValuePair in reduceKeyToCount)
            {
                // those are the remaining keys that weren't used,
                // reduce keys that were replaced
                actions.MapReduce.IncrementReduceKeyCounter(indexId, keyValuePair.Key, -keyValuePair.Value);
            }

            actions.General.MaybePulseTransaction();

            var parallelReductionOperations = new ConcurrentQueue<ParallelBatchStats>();
            var parallelReductionStart = SystemTime.UtcNow;

            context.Database.MappingThreadPool.ExecuteBatch(changed, enumerator =>
                context.TransactionalStorage.Batch(accessor =>
                {
                    var parallelStats = new ParallelBatchStats
                    {
                        StartDelay = (long) (SystemTime.UtcNow - parallelReductionStart).TotalMilliseconds
                    };

                    var scheduleReductionsDuration = new Stopwatch();

                    using (StopwatchScope.For(scheduleReductionsDuration))
                    {
                        while (enumerator.MoveNext())
                        {
                            accessor.MapReduce.ScheduleReductions(indexId, 0, enumerator.Current);
                            accessor.General.MaybePulseTransaction();
                        }
                    }

                    parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Map_ScheduleReductions, scheduleReductionsDuration.ElapsedMilliseconds));
                    parallelReductionOperations.Enqueue(parallelStats);
                }),
                description: $"Schedule reductions for index {PublicName} after operation " +
                             $"from etag {GetLastEtagFromStats()} to etag {batch.HighestEtagBeforeFiltering}");

            performanceStats.Add(new ParallelPerformanceStats
            {
                NumberOfThreads = parallelReductionOperations.Count,
                DurationMs = (long)(SystemTime.UtcNow - parallelReductionStart).TotalMilliseconds,
                BatchedOperations = parallelReductionOperations.ToList()
            });

            UpdateIndexingStats(context, stats);

            performance.OnCompleted = () => BatchCompleted("Current Map", "Map", sourceCount, count, performanceStats);
            if (logIndexing.IsDebugEnabled)
                logIndexing.Debug("Mapped {0} documents for {1}", count, PublicName);

            return performance;
        }

        private int ProcessBatch(AbstractViewGenerator viewGenerator, List<object> currentDocumentResults, string currentKey, HashSet<ReduceKeyAndBucket> changes,
            IStorageActionsAccessor actions,
            IDictionary<string, int> statsPerKey, Stopwatch reduceDuringMapLinqExecution, Stopwatch putMappedResultsDuration, Stopwatch convertToRavenJObjectDuration)
        {
            if (currentKey == null || currentDocumentResults.Count == 0)
            {
                return 0;
            }

            var old = CurrentIndexingScope.Current;
            try
            {
                CurrentIndexingScope.Current = null;

                if (logIndexing.IsDebugEnabled)
                {
                    var sb = new StringBuilder()
                        .AppendFormat("Index {0} for document {1} resulted in:", PublicName, currentKey)
                        .AppendLine();
                    foreach (var currentDocumentResult in currentDocumentResults)
                    {
                        sb.AppendLine(JsonConvert.SerializeObject(currentDocumentResult));
                    }
                    logIndexing.Debug(sb.ToString());
                }

                int count = 0;

                var results = RobustEnumerationReduceDuringMapPhase(currentDocumentResults.GetEnumerator(), viewGenerator.ReduceDefinition, reduceDuringMapLinqExecution);
                foreach (var doc in results)
                {
                    count++;

                    var reduceValue = viewGenerator.GroupByExtraction(doc);
                    if (reduceValue == null)
                    {
                        if (logIndexing.IsDebugEnabled)
                            logIndexing.Debug("Field {0} is used as the reduce key and cannot be null, skipping document {1}",
                                              viewGenerator.GroupByExtraction, currentKey);
                        continue;
                    }
                    string reduceKey = ReduceKeyToString(reduceValue);

                    RavenJObject data;
                    using (StopwatchScope.For(convertToRavenJObjectDuration))
                    {
                        data = GetMappedData(doc);
                    }

                    if (logIndexing.IsDebugEnabled)
                    {
                        logIndexing.Debug("Index {0} for document {1} resulted in ({2}): {3}", PublicName, currentKey, reduceKey, data);
                    }

                    using (StopwatchScope.For(putMappedResultsDuration))
                    {
                        actions.MapReduce.PutMappedResult(indexId, currentKey, reduceKey, data);
                    }

                    statsPerKey[reduceKey] = statsPerKey.GetOrDefault(reduceKey) + 1;
                    actions.General.MaybePulseTransaction();
                    changes.Add(new ReduceKeyAndBucket(IndexingUtil.MapBucket(currentKey), reduceKey));
                }
                return count;
            }
            finally
            {
                CurrentIndexingScope.Current = old;
            }
        }

        private RavenJObject GetMappedData(object doc)
        {
            if (doc is IDynamicJsonObject)
                return ((IDynamicJsonObject)doc).Inner;

            var ravenJTokenWriter = new RavenJTokenWriter();
            jsonSerializer.Serialize(ravenJTokenWriter, doc);
            return (RavenJObject)ravenJTokenWriter.Token;
        }

        private static readonly ConcurrentDictionary<Type, Func<object, object>> documentIdFetcherCache =
            new ConcurrentDictionary<Type, Func<object, object>>();

        private static string GetDocumentId(object doc)
        {
            var docIdFetcher = documentIdFetcherCache.GetOrAdd(doc.GetType(), type =>
            {
                // document may be DynamicJsonObject if we are using compiled views
                if (typeof(DynamicJsonObject) == type)
                {
                    return i => ((dynamic)i).__document_id;
                }
                var docIdProp = TypeDescriptor.GetProperties(doc).Find(Constants.DocumentIdFieldName, false);
                return docIdProp.GetValue;
            });
            if (docIdFetcher == null)
                throw new InvalidOperationException("Could not create document id fetcher for this document");
            var documentId = docIdFetcher(doc);
            if (documentId == null || documentId is DynamicNullObject)
                throw new InvalidOperationException("Could not get document id fetcher for this document");

            return (string)documentId;
        }

        internal static string ReduceKeyToString(object reduceValue)
        {
            var reduceValueAsString = reduceValue as string;
            if (reduceValueAsString != null)
                return reduceValueAsString;

            if (reduceValue is DateTime)
                return ((DateTime)reduceValue).GetDefaultRavenFormat();
            if (reduceValue is DateTimeOffset)
                return ((DateTimeOffset)reduceValue).ToString(Default.DateTimeFormatsToWrite, CultureInfo.InvariantCulture);
            if (reduceValue is ValueType)
                return reduceValue.ToString();

            var dynamicJsonObject = reduceValue as IDynamicJsonObject;
            if (dynamicJsonObject != null)
                return dynamicJsonObject.Inner.ToString(Formatting.None);

            return RavenJToken.FromObject(reduceValue).ToString(Formatting.None);
        }

        protected override IndexQueryResult RetrieveDocument(Document document, FieldsToFetch fieldsToFetch, ScoreDoc score)
        {
            fieldsToFetch.EnsureHasField(Constants.ReduceKeyFieldName);
            if (fieldsToFetch.HasExplicitFieldsToFetch)
            {
                return base.RetrieveDocument(document, fieldsToFetch, score);
            }
            var field = document.GetField(Constants.ReduceValueFieldName);
            if (field == null)
            {
                fieldsToFetch = fieldsToFetch.CloneWith(document.GetFields().Select(x => x.Name).ToArray());
                return base.RetrieveDocument(document, fieldsToFetch, score);
            }
            var projection = RavenJObject.Parse(field.StringValue);
            if (fieldsToFetch.FetchAllStoredFields)
            {
                var fields = new HashSet<string>(document.GetFields().Select(x => x.Name));
                fields.Remove(Constants.ReduceKeyFieldName);
                var documentFromFields = new RavenJObject();
                AddFieldsToDocument(document, fields, documentFromFields);
                foreach (var kvp in projection)
                {
                    documentFromFields[kvp.Key] = kvp.Value;
                }
                projection = documentFromFields;
            }
            return new IndexQueryResult
            {
                Projection = projection,
                Score = score.Score,
                ReduceVal = field.StringValue
            };
        }

        protected override void HandleCommitPoints(IndexedItemsInfo itemsInfo, IndexSegmentsInfo segmentsInfo)
        {
            // MapReduce index does not store and use any commit points
        }

        protected override bool IsUpToDateEnoughToWriteToDisk(Etag highestETag)
        {
            // for map/reduce indexes, we always write to disk, the in memory optimization
            // isn't really doing much for us, since we already write the intermediate results 
            // to disk anyway, so it doesn't matter
            return true;
        }

        public override void Remove(string[] keys, WorkContext context)
        {
            DeletionBatchInfo deletionBatchInfo = null;
            try
            {
                deletionBatchInfo = context.ReportDeletionBatchStarted(PublicName, keys.Length);

                context.TransactionalStorage.Batch(actions =>
                {
                    var storageCommitDuration = new Stopwatch();

                    actions.BeforeStorageCommit += storageCommitDuration.Start;

                    actions.AfterStorageCommit += () =>
                    {
                        storageCommitDuration.Stop();

                        deletionBatchInfo.PerformanceStats.Add(PerformanceStats.From(IndexingOperation.StorageCommit, storageCommitDuration.ElapsedMilliseconds));
                    };

                    var reduceKeyAndBuckets = new Dictionary<ReduceKeyAndBucket, int>();

                    var deleteMappedResultsDuration = new Stopwatch();

                    using (StopwatchScope.For(deleteMappedResultsDuration))
                    {
                        if (actions.MapReduce.HasMappedResultsForIndex(indexId))
                        {
                            foreach (var key in keys)
                            {
                                actions.MapReduce.DeleteMappedResultsForDocumentId(key, indexId, reduceKeyAndBuckets);
                                context.CancellationToken.ThrowIfCancellationRequested();
                            }
                        }
                    }

                    deletionBatchInfo.PerformanceStats.Add(PerformanceStats.From(IndexingOperation.Delete_DeleteMappedResultsForDocumentId, deleteMappedResultsDuration.ElapsedMilliseconds));

                    actions.MapReduce.UpdateRemovedMapReduceStats(indexId, reduceKeyAndBuckets, context.CancellationToken);

                    var scheduleReductionsDuration = new Stopwatch();

                    using (StopwatchScope.For(scheduleReductionsDuration))
                    {
                        foreach (var reduceKeyAndBucket in reduceKeyAndBuckets)
                        {
                            actions.MapReduce.ScheduleReductions(indexId, 0, reduceKeyAndBucket.Key);
                            context.CancellationToken.ThrowIfCancellationRequested();
                        }
                    }

                    deletionBatchInfo.PerformanceStats.Add(PerformanceStats.From(IndexingOperation.Reduce_ScheduleReductions, scheduleReductionsDuration.ElapsedMilliseconds));
                });
            }
            finally
            {
                if (deletionBatchInfo != null)
                {
                    context.ReportDeletionBatchCompleted(deletionBatchInfo);
                }
            }
            
        }

        public class ReduceDocuments
        {
            private readonly MapReduceIndex parent;
            private readonly int inputCount;
            private readonly int indexId;
            private readonly AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter;
            private readonly Document luceneDoc = new Document();

            private readonly Field reduceValueField = new Field(Constants.ReduceValueFieldName, "dummy", Field.Store.YES, Field.Index.NO);
            private readonly Field reduceKeyField = new Field(Constants.ReduceKeyFieldName, "dummy", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);

            private readonly ConcurrentDictionary<Type, PropertyAccessor> propertyAccessorCache = new ConcurrentDictionary<Type, PropertyAccessor>();
            private readonly List<AbstractIndexUpdateTriggerBatcher> batchers;

            public ReduceDocuments(MapReduceIndex parent, AbstractViewGenerator viewGenerator, IEnumerable<IGrouping<int, object>> mappedResultsByBucket, int level, WorkContext context, IStorageActionsAccessor actions, HashSet<string> reduceKeys, int inputCount)
            {
                this.parent = parent;
                this.inputCount = inputCount;
                indexId = this.parent.indexId;
                ViewGenerator = viewGenerator;
                MappedResultsByBucket = mappedResultsByBucket;
                Level = level;
                Context = context;
                Actions = actions;
                ReduceKeys = reduceKeys;

                anonymousObjectToLuceneDocumentConverter = new AnonymousObjectToLuceneDocumentConverter(this.parent.context.Database, this.parent.indexDefinition, ViewGenerator, logIndexing);

                if (Level == 2)
                {
                    batchers = Context.IndexUpdateTriggers.Select(x => x.CreateBatcher(indexId))
                                .Where(x => x != null)
                                .ToList();
                }
            }

            public AbstractViewGenerator ViewGenerator { get; private set; }
            public IEnumerable<IGrouping<int, object>> MappedResultsByBucket { get; private set; }
            public int Level { get; private set; }
            public WorkContext Context { get; private set; }
            public IStorageActionsAccessor Actions { get; private set; }
            public HashSet<string> ReduceKeys { get; private set; }

            private string ExtractReduceKey(AbstractViewGenerator viewGenerator, object doc)
            {
                try
                {
                    object reduceKey = viewGenerator.GroupByExtraction(doc);
                    if (reduceKey == null)
                        throw new InvalidOperationException("Could not find reduce key for " + parent.PublicName + " in the result: " + doc);

                    return ReduceKeyToString(reduceKey);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not extract reduce key from reduce result!", e);
                }
            }

            private IEnumerable<AbstractField> GetFields(object doc, out float boost)
            {
                boost = 1;
                var boostedValue = doc as BoostedValue;
                if (boostedValue != null)
                {
                    doc = boostedValue.Value;
                    boost = boostedValue.Boost;
                }

                IEnumerable<AbstractField> fields = null;
                try
                {
                    var dynamicJsonObject = doc as IDynamicJsonObject;
                    if (dynamicJsonObject != null)
                    {
                        fields = anonymousObjectToLuceneDocumentConverter.Index(dynamicJsonObject.Inner, Field.Store.NO);
                    }
                    else
                    {
                        var properties = propertyAccessorCache.GetOrAdd(doc.GetType(), PropertyAccessor.Create);
                        fields = anonymousObjectToLuceneDocumentConverter.Index(doc, properties, Field.Store.NO);
                    }
                }
                catch (InvalidShapeException)
                {
                }

                if (Math.Abs(boost - 1) > float.Epsilon)
                {
                    return fields.Select(x => { x.OmitNorms = false; return x; });
                }
                return fields;
            }

            private static RavenJObject ToJsonDocument(object doc)
            {
                var boostedValue = doc as BoostedValue;
                if (boostedValue != null)
                {
                    doc = boostedValue.Value;
                }
                var dynamicJsonObject = doc as IDynamicJsonObject;
                if (dynamicJsonObject != null)
                {
                    return dynamicJsonObject.Inner;
                }
                var ravenJObject = doc as RavenJObject;
                if (ravenJObject != null)
                    return ravenJObject;
                var jsonDocument = RavenJObject.FromObject(doc);
                MergeArrays(jsonDocument);

                // remove _, __, etc fields
                foreach (var prop in jsonDocument.Where(x => x.Key.All(ch => ch == '_')).ToArray())
                {
                    jsonDocument.Remove(prop.Key);
                }
                return jsonDocument;
            }

            private static void MergeArrays(RavenJToken token)
            {
                if (token == null)
                    return;
                switch (token.Type)
                {
                    case JTokenType.Array:
                        var arr = (RavenJArray)token;
                        for (int i = 0; i < arr.Length; i++)
                        {
                            var current = arr[i];
                            if (current == null || current.Type != JTokenType.Array)
                                continue;
                            arr.RemoveAt(i);
                            i--;
                            var j = Math.Max(0, i);
                            foreach (var item in (RavenJArray)current)
                            {
                                arr.Insert(j++, item);
                            }
                        }
                        break;
                    case JTokenType.Object:
                        foreach (var kvp in ((RavenJObject)token))
                        {
                            MergeArrays(kvp.Value);
                        }
                        break;
                }
            }

            public IndexingPerformanceStats ExecuteReduction()
            {
                var count = 0;
                var sourceCount = 0;
                var addDocumentDuration = new Stopwatch();
                var convertToLuceneDocumentDuration = new Stopwatch();
                var linqExecutionDuration = new Stopwatch();
                var deleteExistingDocumentsDuration = new Stopwatch();
                var writeToIndexStats = new List<PerformanceStats>();

                IndexingPerformanceStats performance = null;

                parent.Write((indexWriter, analyzer, stats) =>
                {
                    stats.Operation = IndexingWorkStats.Status.Reduce;

                    try
                    {
                        if (Level == 2)
                        {
                            RemoveExistingReduceKeysFromIndex(indexWriter, deleteExistingDocumentsDuration);
                        }

                        foreach (var mappedResults in MappedResultsByBucket)
                        {
                            var input = mappedResults.Select(x =>
                            {
                                sourceCount++;
                                return x;
                            });

                            IndexingFunc reduceDefinition = ViewGenerator.ReduceDefinition;
                            foreach (var doc in parent.RobustEnumerationReduce(input.GetEnumerator(), reduceDefinition, stats, linqExecutionDuration))
                            {
                                count++;

                                switch (Level)
                                {
                                    case 0:
                                    case 1:
                                        string reduceKeyAsString = ExtractReduceKey(ViewGenerator, doc);
                                        Actions.MapReduce.PutReducedResult(indexId, reduceKeyAsString, Level + 1, mappedResults.Key, mappedResults.Key / 1024, ToJsonDocument(doc));
                                        Actions.General.MaybePulseTransaction();
                                        break;
                                    case 2:
                                        WriteDocumentToIndex(doc, indexWriter, analyzer, convertToLuceneDocumentDuration, addDocumentDuration);
                                        break;
                                    default:
                                        throw new InvalidOperationException("Unknown level: " + Level);
                                }

                                stats.ReduceSuccesses++;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        if (Level == 2)
                        {
                            batchers.ApplyAndIgnoreAllErrors(
                                ex =>
                                {
                                    logIndexing.WarnException("Failed to notify index update trigger batcher about an error in " + parent.PublicName, ex);
                                    Context.AddError(indexId, parent.indexDefinition.Name, null, ex, "AnErrorOccured Trigger");
                                },
                                x => x.AnErrorOccured(e));
                        }
                        throw;
                    }
                    finally
                    {
                        if (Level == 2)
                        {
                            batchers.ApplyAndIgnoreAllErrors(
                                e =>
                                {
                                    logIndexing.WarnException("Failed to dispose on index update trigger in " + parent.PublicName, e);
                                    Context.AddError(indexId, parent.indexDefinition.Name, null, e, "Dispose Trigger");
                                },
                                x => x.Dispose());
                        }

                        // TODO: Check if we need to report "Bucket Counts" or "Total Input Elements"?
                        performance = parent.RecordCurrentBatch("Current Reduce #" + Level, "Reduce Level " + Level, sourceCount);
                    }

                    return new IndexedItemsInfo(null)
                    {
                        ChangedDocs = count + ReduceKeys.Count
                    };
                }, writeToIndexStats);

                var performanceStats = new List<BasePerformanceStats>();

                performanceStats.Add(PerformanceStats.From(IndexingOperation.Linq_ReduceLinqExecution, linqExecutionDuration.ElapsedMilliseconds));
                performanceStats.Add(PerformanceStats.From(IndexingOperation.Lucene_DeleteExistingDocument, deleteExistingDocumentsDuration.ElapsedMilliseconds));
                performanceStats.Add(PerformanceStats.From(IndexingOperation.Lucene_ConvertToLuceneDocument, convertToLuceneDocumentDuration.ElapsedMilliseconds));
                performanceStats.Add(PerformanceStats.From(IndexingOperation.Lucene_AddDocument, addDocumentDuration.ElapsedMilliseconds));
                performanceStats.AddRange(writeToIndexStats);

                parent.BatchCompleted("Current Reduce #" + Level, "Reduce Level " + Level, sourceCount, count, performanceStats);
                if (logIndexing.IsDebugEnabled)
                    logIndexing.Debug(() => string.Format("Reduce resulted in {0} entries for {1} for reduce keys at level {3}: {2}", count, parent.PublicName, string.Join(", ", ReduceKeys), Level));

                return performance;
            }

            private void WriteDocumentToIndex(object doc, RavenIndexWriter indexWriter, Analyzer analyzer, Stopwatch convertToLuceneDocumentDuration, Stopwatch addDocumentDutation)
            {
                string reduceKeyAsString;
                using (StopwatchScope.For(convertToLuceneDocumentDuration))
                {
                    float boost;
                    try
                    {
                        var fields = GetFields(doc, out boost);

                        reduceKeyAsString = ExtractReduceKey(ViewGenerator, doc);
                        reduceKeyField.SetValue(reduceKeyAsString);
                        reduceValueField.SetValue(ToJsonDocument(doc).ToString(Formatting.None));

                        luceneDoc.GetFields().Clear();
                        luceneDoc.Boost = boost;
                        luceneDoc.Add(reduceKeyField);
                        luceneDoc.Add(reduceValueField);

                        foreach (var field in fields)
                            luceneDoc.Add(field);
                    }
                    catch (Exception e)
                    {
                        Context.AddError(indexId,
                            parent.PublicName,
                            TryGetDocKey(doc),
                            e,
                            "Reduce"
                            );
                        logIndexing.WarnException("Could not get fields to during reduce for " + parent.PublicName, e);
                        return;
                    }
                }
                batchers.ApplyAndIgnoreAllErrors(
                    exception =>
                    {
                        logIndexing.WarnException(
                            string.Format("Error when executed OnIndexEntryCreated trigger for index '{0}', key: '{1}'",
                                          parent.PublicName, reduceKeyAsString),
                            exception);
                        Context.AddError(indexId, parent.PublicName, reduceKeyAsString, exception, "OnIndexEntryCreated Trigger");
                    },
                    trigger => trigger.OnIndexEntryCreated(reduceKeyAsString, luceneDoc));

                parent.LogIndexedDocument(reduceKeyAsString, luceneDoc);

                using (StopwatchScope.For(addDocumentDutation))
                {
                    parent.AddDocumentToIndex(indexWriter, luceneDoc, analyzer);
                }
            }

            private void RemoveExistingReduceKeysFromIndex(RavenIndexWriter indexWriter, Stopwatch deleteExistingDocumentsDuration)
            {
                foreach (var reduceKey in ReduceKeys)
                {
                    var entryKey = reduceKey;
                    parent.InvokeOnIndexEntryDeletedOnAllBatchers(batchers, new Term(Constants.ReduceKeyFieldName, entryKey));

                    using (StopwatchScope.For(deleteExistingDocumentsDuration))
                    {
                        indexWriter.DeleteDocuments(new Term(Constants.ReduceKeyFieldName, entryKey));
                    }
                }
            }
        }
    }
}
