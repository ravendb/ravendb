// -----------------------------------------------------------------------
//  <copyright file="QueryActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Database.Actions
{
    public class QueryActions : ActionsBase
    {
        public QueryActions(DocumentDatabase database, IUuidGenerator uuidGenerator, ILog log)
            : base(database, uuidGenerator, log)
        {
        }

        public HashSet<string> QueryDocumentIds(string index, IndexQuery query, CancellationTokenSource tokenSource, out bool stale)
        {
            var queryStat = AddToCurrentlyRunningQueryList(index, query, tokenSource);
            try
            {
                bool isStale = false;
                HashSet<string> loadedIds = null;
                TransactionalStorage.Batch(
                    actions =>
                    {
                        var definition = IndexDefinitionStorage.GetIndexDefinition(index);
                        if (definition == null)
                            throw new ArgumentException("specified index definition was not found", "index");

                        isStale = actions.Staleness.IsIndexStale(definition.IndexId, query.Cutoff, null);

                        if (isStale == false && query.Cutoff == null)
                        {
                            var indexInstance = Database.IndexStorage.GetIndexInstance(index);
                            isStale = isStale || (indexInstance != null && indexInstance.IsMapIndexingInProgress);
                        }

                        if (isStale && actions.Staleness.IsIndexStaleByTask(definition.IndexId, query.Cutoff) == false &&
                            actions.Staleness.IsReduceStale(definition.IndexId) == false)
                        {
                            var viewGenerator = IndexDefinitionStorage.GetViewGenerator(index);
                            if (viewGenerator == null)
                                throw new ArgumentException("specified index definition was not found", "index");

                            var forEntityNames = viewGenerator.ForEntityNames.ToList();
                            var lastIndexedEtag = actions.Indexing.GetIndexStats(definition.IndexId).LastIndexedEtag;

                            if (Database.LastCollectionEtags.HasEtagGreaterThan(forEntityNames, lastIndexedEtag) == false)
                                isStale = false;
                        }

                        var indexFailureInformation = actions.Indexing.GetFailureRate(definition.IndexId);

                        if (indexFailureInformation.IsInvalidIndex)
                        {
                            throw new IndexDisabledException(indexFailureInformation);
                        }
                        loadedIds = new HashSet<string>(from queryResult in Database.IndexStorage.Query(index, query, result => true, new FieldsToFetch(null, false, Constants.DocumentIdFieldName), Database.IndexQueryTriggers, tokenSource.Token)
                                                        select queryResult.Key);
                    });
                stale = isStale;
                return loadedIds;
            }
            finally
            {
                RemoveFromCurrentlyRunningQueryList(index, queryStat);
            }
        }


        public QueryResultWithIncludes Query(string index, IndexQuery query, CancellationToken externalCancellationToken)
        {
            DateTime? showTimingByDefaultUntil = WorkContext.ShowTimingByDefaultUntil;
            if (showTimingByDefaultUntil != null)
            {
                if (showTimingByDefaultUntil < SystemTime.UtcNow) // expired, reset
                {
                    WorkContext.ShowTimingByDefaultUntil = null;
                }
                else
                {
                    query.ShowTimings = true;
                }
            }

            QueryResultWithIncludes result = null;
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken, WorkContext.CancellationToken))
            {
                TransactionalStorage.Batch(
                    accessor =>
                    {
                        using (var op = new DatabaseQueryOperation(Database, index, query, accessor, cts)
                        {
                            ShouldSkipDuplicateChecking = query.SkipDuplicateChecking
                        })
                        {
                            var list = new List<RavenJObject>();
                            op.Init();
                            op.Execute(list.Add);
                            op.Result.Results = list;
                            result = op.Result;
                        }
                    });
            }

            return result;
        }

        public class DatabaseQueryOperation : IDisposable
        {
            public bool ShouldSkipDuplicateChecking = false;
            private readonly DocumentDatabase database;
            private readonly string indexName;
            private readonly IndexQuery query;
            private readonly IStorageActionsAccessor actions;

            private readonly CancellationToken cancellationToken;

            private readonly ExecutingQueryInfo queryStat;
            public QueryResultWithIncludes Result = new QueryResultWithIncludes();
            public QueryHeaderInformation Header;
            private bool stale;
            private IEnumerable<RavenJObject> results;
            private DocumentRetriever docRetriever;
            internal DocumentRetriever DocRetriever
            {
                get { return docRetriever; }
            }
            private Stopwatch duration;
            private List<string> transformerErrors;
            private bool nonAuthoritativeInformation;
            private Etag resultEtag;
            private Tuple<DateTime, Etag> indexTimestamp;
            private Dictionary<string, Dictionary<string, string[]>> highlightings;
            private Dictionary<string, string> scoreExplanations;
            private HashSet<string> idsToLoad;

            private readonly ILog logger = LogManager.GetCurrentClassLogger();

            private readonly Dictionary<QueryTimings, double> executionTimes = new Dictionary<QueryTimings, double>();

            public DocumentDatabase Database
            {
                get { return database; }
            }
            public DatabaseQueryOperation(DocumentDatabase database, string indexName, IndexQuery query, IStorageActionsAccessor actions, CancellationTokenSource cancellationTokenSource)
            {
                this.database = database;
                this.indexName = indexName != null ? indexName.Trim() : null;
                this.query = query;
                this.actions = actions;
                cancellationToken = cancellationTokenSource.Token;
                queryStat = database.Queries.AddToCurrentlyRunningQueryList(indexName, query, cancellationTokenSource);

                if (query.ShowTimings)
                {
                    executionTimes[QueryTimings.Lucene] = 0;
                    executionTimes[QueryTimings.LoadDocuments] = 0;
                    executionTimes[QueryTimings.TransformResults] = 0;
                }
            }

            public void Init()
            {
                try
                {
                    highlightings = new Dictionary<string, Dictionary<string, string[]>>();
                    scoreExplanations = new Dictionary<string, string>();
                    Func<IndexQueryResult, object> tryRecordHighlightingAndScoreExplanation = queryResult =>
                    {
                  
                        if (queryResult.Highligtings != null && (queryResult.Key != null || queryResult.HighlighterKey != null))
                            highlightings.Add(queryResult.Key ?? queryResult.HighlighterKey, queryResult.Highligtings);
                        if ((queryResult.Key != null || queryResult.ReduceVal != null) && queryResult.ScoreExplanation != null)
                            scoreExplanations.Add(queryResult.Key ?? queryResult.ReduceVal, queryResult.ScoreExplanation);
                        return null;
                    };
                    stale = false;
                    indexTimestamp = Tuple.Create(DateTime.MinValue, Etag.Empty);
                    resultEtag = Etag.Empty;
                    nonAuthoritativeInformation = false;

                    if (string.IsNullOrEmpty(query.ResultsTransformer) == false &&
                        (query.FieldsToFetch == null || query.FieldsToFetch.Length == 0))
                    {
                        query.FieldsToFetch = new[] { Constants.AllFields };
                    }

                    duration = Stopwatch.StartNew();
                    idsToLoad = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                    var viewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
                    var index = database.IndexDefinitionStorage.GetIndexDefinition(indexName);
                    if (viewGenerator == null)
                        throw new IndexDoesNotExistsException("Could not find index named: " + indexName);

                    resultEtag = database.Indexes.GetIndexEtag(index.Name, null, query.ResultsTransformer);

                    stale = actions.Staleness.IsIndexStale(index.IndexId, query.Cutoff, query.CutoffEtag);

                    if (stale == false && query.Cutoff == null && query.CutoffEtag == null)
                    {
                        var indexInstance = database.IndexStorage.GetIndexInstance(indexName);
                        stale = stale || (indexInstance != null && indexInstance.IsMapIndexingInProgress);
                    }

                    if (stale &&
                        actions.Staleness.IsIndexStaleByTask(index.IndexId, query.Cutoff) == false &&
                        actions.Staleness.IsReduceStale(index.IndexId) == false)
                    {
                        var forEntityNames = viewGenerator.ForEntityNames.ToList();
                        var lastIndexedEtag = actions.Indexing.GetIndexStats(index.IndexId).LastIndexedEtag;

                        if (database.LastCollectionEtags.HasEtagGreaterThan(forEntityNames, lastIndexedEtag) == false)
                            stale = false;
                    }

                    indexTimestamp = actions.Staleness.IndexLastUpdatedAt(index.IndexId);
                    var indexFailureInformation = actions.Indexing.GetFailureRate(index.IndexId);
                    if (indexFailureInformation.IsInvalidIndex)
                    {
                        throw new IndexDisabledException(indexFailureInformation);
                    }
                    docRetriever = new DocumentRetriever(database.Configuration, actions, database.ReadTriggers, query.TransformerParameters, idsToLoad);
                    var fieldsToFetch = new FieldsToFetch(query,
                        viewGenerator.ReduceDefinition == null
                            ? Constants.DocumentIdFieldName
                            : Constants.ReduceKeyFieldName);
                    Func<IndexQueryResult, bool> shouldIncludeInResults =
                        result => docRetriever.ShouldIncludeResultInQuery(result, index, fieldsToFetch, ShouldSkipDuplicateChecking);
                    var indexQueryResults = database.IndexStorage.Query(indexName, query, shouldIncludeInResults,
                        fieldsToFetch, database.IndexQueryTriggers, cancellationToken, (query.ShowTimings ? (Action<double>)(time => executionTimes[QueryTimings.Parse] = time) : null));
                    if (query.ShowTimings)
                    {
                        indexQueryResults = new TimedEnumerable<IndexQueryResult>(indexQueryResults, timeInMilliseconds => executionTimes[QueryTimings.Lucene] += timeInMilliseconds);
                    } 
                    indexQueryResults = new ActiveEnumerable<IndexQueryResult>(indexQueryResults);
               

                    var docs = from queryResult in indexQueryResults
                        let doc = docRetriever.RetrieveDocumentForQuery(queryResult, index, fieldsToFetch, ShouldSkipDuplicateChecking)
                        where doc != null
                        let _ = nonAuthoritativeInformation |= (doc.NonAuthoritativeInformation ?? false)
                        let __ = tryRecordHighlightingAndScoreExplanation(queryResult)
                        select doc;

                    transformerErrors = new List<string>();
                    results = database
                        .Queries
                        .GetQueryResults(query, viewGenerator, docRetriever, docs, transformerErrors,
                            timeInMilliseconds => executionTimes[QueryTimings.LoadDocuments] = timeInMilliseconds,
                            timeInMilliseconds => executionTimes[QueryTimings.TransformResults] = timeInMilliseconds,
                            query.ShowTimings, cancellationToken);

                    Header = new QueryHeaderInformation
                    {
                        Index = indexName,
                        IsStale = stale,
                        ResultEtag = resultEtag,
                        IndexTimestamp = indexTimestamp.Item1,
                        IndexEtag = indexTimestamp.Item2,
                        TotalResults = query.TotalSize.Value
                    };
                }
                catch (Exception)
                {
                    Dispose();
                    throw;
                }
            }

            public void Execute(Action<RavenJObject> onResult)
            {
                using (new CurrentTransformationScope(database, docRetriever))
                {
                    foreach (var result in results)
                    {						
                        cancellationToken.ThrowIfCancellationRequested();
                        database.WorkContext.UpdateFoundWork();

                        onResult(result);
                    }
                    if (transformerErrors.Count > 0)
                    {
                        logger.Error("The transform results function failed.\r\n{0}", string.Join("\r\n", transformerErrors));
                        throw new InvalidOperationException("The transform results function failed.\r\n" + string.Join("\r\n", transformerErrors));
                    }
                }

                Result = new QueryResultWithIncludes
                {
                    IndexName = indexName,
                    IsStale = stale,
                    NonAuthoritativeInformation = nonAuthoritativeInformation,
                    SkippedResults = query.SkippedResults.Value,
                    TotalResults = query.TotalSize.Value,
                    IndexTimestamp = indexTimestamp.Item1,
                    IndexEtag = indexTimestamp.Item2,
                    ResultEtag = resultEtag,
                    IdsToInclude = idsToLoad,
                    LastQueryTime = SystemTime.UtcNow,
                    Highlightings = highlightings,
                    DurationMilliseconds = duration.ElapsedMilliseconds,
                    ScoreExplanations = scoreExplanations,
                    TimingsInMilliseconds = NormalizeTimings()
                };
            }

            private Dictionary<string, double> NormalizeTimings()
            {
                if (query.ShowTimings)
                {
                    var luceneTime = executionTimes[QueryTimings.Lucene];
                    var loadDocumentsTime = executionTimes[QueryTimings.LoadDocuments];
                    var transformResultsTime = executionTimes[QueryTimings.TransformResults];

                    executionTimes[QueryTimings.LoadDocuments] -= loadDocumentsTime > 0 ? luceneTime : 0;
                    executionTimes[QueryTimings.TransformResults] -= transformResultsTime > 0 ? loadDocumentsTime + luceneTime : 0;
                }

                return executionTimes.ToDictionary(x => x.Key.GetDescription(), x => x.Value >= 0 ? Math.Round(x.Value) : 0);
            }

            public void Dispose()
            {
                database.Queries.RemoveFromCurrentlyRunningQueryList(indexName, queryStat);
                var resultsAsDisposable = results as IDisposable;
                if (resultsAsDisposable != null)
                    resultsAsDisposable.Dispose();
            }
        }

        private void RemoveFromCurrentlyRunningQueryList(string index, ExecutingQueryInfo queryStat)
        {
            if (index == null)
                return;

            ConcurrentSet<ExecutingQueryInfo> set;
            if (WorkContext.CurrentlyRunningQueries.TryGetValue(index, out set) == false)
                return;

            set.TryRemove(queryStat);
        }

        private ExecutingQueryInfo AddToCurrentlyRunningQueryList(string index, IndexQuery query, CancellationTokenSource externalTokenSource)
        {
            var set = WorkContext.CurrentlyRunningQueries.GetOrAdd(index, x => new ConcurrentSet<ExecutingQueryInfo>());
            var queryStartTime = DateTime.UtcNow;
            var queryId = WorkContext.GetNextQueryId();
            var executingQueryInfo = new ExecutingQueryInfo(queryStartTime, query, queryId, externalTokenSource);
            set.Add(executingQueryInfo);
            return executingQueryInfo;
        }

        private IEnumerable<RavenJObject> GetQueryResults(IndexQuery query, AbstractViewGenerator viewGenerator, DocumentRetriever docRetriever, IEnumerable<JsonDocument> results, List<string> transformerErrors, Action<double> loadingDocumentsFinish, Action<double> transformerFinish, bool showTimings, CancellationToken token)
        {
            if (query.PageSize <= 0) // maybe they just want the stats? 
            {
                return Enumerable.Empty<RavenJObject>();
            }

            IndexingFunc transformFunc = null;
            
            // Check an explicitly declared one first
            if (string.IsNullOrEmpty(query.ResultsTransformer) == false)
            {
                var transformGenerator = IndexDefinitionStorage.GetTransformer(query.ResultsTransformer);

                if (transformGenerator != null && transformGenerator.TransformResultsDefinition != null)
                    transformFunc = transformGenerator.TransformResultsDefinition;
                else
                    throw new InvalidOperationException("The transformer " + query.ResultsTransformer + " was not found");
            }

            if (transformFunc == null)
            {
                var resultsWithoutTransformer = results.Select(x =>
                {
                    var ravenJObject = x.ToJson();
                    if (query.IsDistinct)
                    {
                        ravenJObject[Constants.DocumentIdFieldName] = x.Key;
                    }
                    var metadata = ravenJObject.Value<RavenJObject>(Constants.Metadata);
                    if (metadata == null) //precaution, should not happen
                        ravenJObject.Add(Constants.Metadata, RavenJToken.FromObject(new {x.SerializedSizeOnDisk}));
                    else
                        metadata[Constants.SerializedSizeOnDisk] = x.SerializedSizeOnDisk;

                    return ravenJObject;
                });
                return showTimings ? new TimedEnumerable<RavenJObject>(resultsWithoutTransformer, loadingDocumentsFinish) : resultsWithoutTransformer;
            }

            var dynamicJsonObjects = results.Select(x =>
            {
                var ravenJObject = x.ToJson();
                if (query.IsDistinct)
                {
                    ravenJObject[Constants.DocumentIdFieldName] = x.Key;
                }
                return new DynamicLuceneOrParentDocumntObject(docRetriever, ravenJObject);
            });
            var robustEnumerator = new RobustEnumerator(token, 100, 
                onError: (exception, o) => transformerErrors.Add(string.Format("Doc '{0}', Error: {1}", Index.TryGetDocKey(o), exception.Message)));

            var resultsWithTransformer = robustEnumerator
                .RobustEnumeration(dynamicJsonObjects.Cast<object>().GetEnumerator(), transformFunc)
                .Select(JsonExtensions.ToJObject);

            return showTimings ? new TimedEnumerable<RavenJObject>(resultsWithTransformer, transformerFinish) : resultsWithTransformer;
        }
    }
}
