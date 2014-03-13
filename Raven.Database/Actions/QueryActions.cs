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
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class QueryActions
    {
        public IEnumerable<string> QueryDocumentIds(string index, IndexQuery query, CancellationToken token, out bool stale)
        {
            var queryStat = AddToCurrentlyRunningQueryList(index, query);
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
                            var indexInstance = IndexStorage.GetIndexInstance(index);
                            isStale = isStale || (indexInstance != null && indexInstance.IsMapIndexingInProgress);
                        }

                        var indexFailureInformation = actions.Indexing.GetFailureRate(definition.IndexId);

                        if (indexFailureInformation.IsInvalidIndex)
                        {
                            throw new IndexDisabledException(indexFailureInformation);
                        }
                        loadedIds = new HashSet<string>(from queryResult in IndexStorage.Query(index, query, result => true, new FieldsToFetch(null, false, Constants.DocumentIdFieldName), IndexQueryTriggers, token)
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
            QueryResultWithIncludes result = null;
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken, workContext.CancellationToken))
            {
                var cancellationToken = cts.Token;

                TransactionalStorage.Batch(
                    accessor =>
                    {
                        using (var op = new DatabaseQueryOperation(this, index, query, accessor, cancellationToken)
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
            private Stopwatch duration;
            private List<string> transformerErrors;
            private bool nonAuthoritativeInformation;
            private Etag resultEtag;
            private Tuple<DateTime, Etag> indexTimestamp;
            private Dictionary<string, Dictionary<string, string[]>> highlightings;
            private Dictionary<string, string> scoreExplanations;
            private HashSet<string> idsToLoad;

            public DatabaseQueryOperation(DocumentDatabase database, string indexName, IndexQuery query, IStorageActionsAccessor actions, CancellationToken cancellationToken)
            {
                this.database = database;
                this.indexName = indexName != null ? indexName.Trim() : null;
                this.query = query;
                this.actions = actions;
                this.cancellationToken = cancellationToken;
                queryStat = database.AddToCurrentlyRunningQueryList(indexName, query);
            }

            public void Init()
            {
                highlightings = new Dictionary<string, Dictionary<string, string[]>>();
                scoreExplanations = new Dictionary<string, string>();
                Func<IndexQueryResult, object> tryRecordHighlightingAndScoreExplanation = queryResult =>
                {
                    if (queryResult.Key == null)
                        return null;
                    if (queryResult.Highligtings != null)
                        highlightings.Add(queryResult.Key, queryResult.Highligtings);
                    if (queryResult.ScoreExplanation != null)
                        scoreExplanations.Add(queryResult.Key, queryResult.ScoreExplanation);
                    return null;
                };
                stale = false;
                indexTimestamp = Tuple.Create(DateTime.MinValue, Etag.Empty);
                resultEtag = Etag.Empty;
                nonAuthoritativeInformation = false;

                if (string.IsNullOrEmpty(query.ResultsTransformer) == false)
                {
                    query.FieldsToFetch = new[] { Constants.AllFields };
                }

                duration = Stopwatch.StartNew();
                idsToLoad = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                var viewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
                var index = database.IndexDefinitionStorage.GetIndexDefinition(indexName);
                if (viewGenerator == null)
                    throw new IndexDoesNotExistsException("Could not find index named: " + indexName);

                resultEtag = database.GetIndexEtag(index.Name, null, query.ResultsTransformer);

                stale = actions.Staleness.IsIndexStale(index.IndexId, query.Cutoff, query.CutoffEtag);

                if (stale == false && query.Cutoff == null && query.CutoffEtag == null)
                {
                    var indexInstance = database.IndexStorage.GetIndexInstance(indexName);
                    stale = stale || (indexInstance != null && indexInstance.IsMapIndexingInProgress);
                }

                indexTimestamp = actions.Staleness.IndexLastUpdatedAt(index.IndexId);
                var indexFailureInformation = actions.Indexing.GetFailureRate(index.IndexId);
                if (indexFailureInformation.IsInvalidIndex)
                {
                    throw new IndexDisabledException(indexFailureInformation);
                }
                docRetriever = new DocumentRetriever(actions, database.ReadTriggers, database.inFlightTransactionalState, query.QueryInputs, idsToLoad);
                var fieldsToFetch = new FieldsToFetch(query.FieldsToFetch, query.IsDistinct,
                    viewGenerator.ReduceDefinition == null
                        ? Constants.DocumentIdFieldName
                        : Constants.ReduceKeyFieldName);
                Func<IndexQueryResult, bool> shouldIncludeInResults =
                    result => docRetriever.ShouldIncludeResultInQuery(result, index, fieldsToFetch, ShouldSkipDuplicateChecking);
                var indexQueryResults = database.IndexStorage.Query(indexName, query, shouldIncludeInResults, fieldsToFetch, database.IndexQueryTriggers, cancellationToken);
                indexQueryResults = new ActiveEnumerable<IndexQueryResult>(indexQueryResults);

                transformerErrors = new List<string>();
                results = database.GetQueryResults(query, viewGenerator, docRetriever,
                    from queryResult in indexQueryResults
                    let doc = docRetriever.RetrieveDocumentForQuery(queryResult, index, fieldsToFetch, ShouldSkipDuplicateChecking)
                    where doc != null
                    let _ = nonAuthoritativeInformation |= (doc.NonAuthoritativeInformation ?? false)
                    let __ = tryRecordHighlightingAndScoreExplanation(queryResult)
                    select doc, transformerErrors, cancellationToken);

                Header = new QueryHeaderInformation
                {
                    Index = indexName,
                    IsStable = stale,
                    ResultEtag = resultEtag,
                    IndexTimestamp = indexTimestamp.Item1,
                    IndexEtag = indexTimestamp.Item2,
                    TotalResults = query.TotalSize.Value
                };
            }

            public void Execute(Action<RavenJObject> onResult)
            {
                using (new CurrentTransformationScope(docRetriever))
                {
                    foreach (var result in results)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        onResult(result);
                    }
                    if (transformerErrors.Count > 0)
                    {
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
                    ScoreExplanations = scoreExplanations
                };
            }

            public void Dispose()
            {
                database.RemoveFromCurrentlyRunningQueryList(indexName, queryStat);
                var resultsAsDisposable = results as IDisposable;
                if (resultsAsDisposable != null)
                    resultsAsDisposable.Dispose();
            }
        }


    }
}