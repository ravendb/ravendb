using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Graph;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries
{
    public partial class GraphQueryRunner : AbstractQueryRunner
    {
        private readonly HashSet<StringSegment> _mapReduceAliases = new HashSet<StringSegment>();

        public GraphQueryRunner(DocumentDatabase database) : base(database)
        {
        }

        public class EdgeDebugInfo
        {
            public string Source;
            public string Destination;
            public object Edge;
        }

        public class GraphDebugInfo
        {
            private readonly DocumentDatabase _database;
            private readonly DocumentsOperationContext _context;
            public Dictionary<string, object> Nodes = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            public Dictionary<string, List<EdgeDebugInfo>> Edges = new Dictionary<string, List<EdgeDebugInfo>>();

            public GraphDebugInfo(DocumentDatabase database, DocumentsOperationContext context)
            {
                _database = database;
                _context = context;
            }

            public void AddEdge(string edgeName, object edge, string source, string dst)
            {
                if (edge == null || dst == null)
                    return;

                if (Edges.TryGetValue(edgeName, out var edgeInfo) == false)
                    Edges[edgeName] = edgeInfo = new List<EdgeDebugInfo>();

                edgeInfo.Add(new EdgeDebugInfo
                {
                    Destination = dst,
                    Source = source ?? "__anonymous__/" + Guid.NewGuid(),
                    Edge = edge
                });

                if (Nodes.TryGetValue(dst, out _) == false)
                {
                    Nodes[dst] = _database.DocumentsStorage.Get(_context, dst);
                }
            }

            public void AddNode(string key, object val)
            {
                key = key ?? "__anonymous__/" + Guid.NewGuid();
                Nodes[key] = val;
            }
        }

        public async Task<GraphDebugInfo> GetAnalyzedQueryResults(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            var qr = await GetQueryResults(query, queryContext, null, token);
            var result = new GraphDebugInfo(Database, queryContext.Documents);
            qr.QueryPlan.Analyze(qr.Matches, result);
            return result;
        }

        public async Task WriteDetailedQueryResult(IndexQueryServerSide indexQuery, QueryOperationContext queryContext, AsyncBlittableJsonTextWriter writer, OperationCancelToken token)
        {
            var qr = await GetQueryResults(indexQuery, queryContext, null, token, true);
            var reporter = new GraphQueryDetailedReporter(writer, queryContext.Documents);
            await reporter.VisitAsync(qr.QueryPlan.RootQueryStep);
        }

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag,
            OperationCancelToken token)
        {
            var res = new DocumentQueryResult
            {
                IndexName = "@graph"
            };
            return await ExecuteQuery(res, query, queryContext, existingResultEtag, token);
        }

        public override Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response, IStreamQueryResultWriter<BlittableJsonReaderObject> writer,
            OperationCancelToken token)
        {
            throw new NotImplementedException();
        }

        private async Task<TResult> ExecuteQuery<TResult>(TResult final, IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token) where TResult : QueryResultServerSide<Document>
        {
            try
            {
                if (Database.ServerStore.Configuration.Core.FeaturesAvailability == FeaturesAvailability.Stable)
                    FeaturesAvailabilityException.Throw("Graph Queries");

                using (QueryRunner.MarkQueryAsRunning(Constants.Documents.Indexing.DummyGraphIndexName, query, token))
                using (var timingScope = new QueryTimingsScope())
                {
                    var qr = await GetQueryResults(query, queryContext, existingResultEtag, token);
                    if (qr.NotModified)
                    {
                        final.NotModified = true;
                        return final;
                    }
                    var q = query.Metadata.Query;

                    //TODO: handle order by, load,  clauses
                    IncludeDocumentsCommand idc = null;
                    IncludeCompareExchangeValuesCommand icevc = null;
                    if (q.Select == null && q.SelectFunctionBody.FunctionText == null)
                    {
                        HandleResultsWithoutSelect(queryContext.Documents, qr.Matches, final);
                    }
                    else if (q.Select != null)
                    {
                        //TODO : investigate fields to fetch
                        var fieldsToFetch = new FieldsToFetch(query, null);
                        idc = new IncludeDocumentsCommand(Database.DocumentsStorage, queryContext.Documents, query.Metadata.Includes, fieldsToFetch.IsProjection);
                        icevc = IncludeCompareExchangeValuesCommand.ExternalScope(queryContext, query.Metadata.CompareExchangeValueIncludes);

                        var resultRetriever = new GraphQueryResultRetriever(
                            q.GraphQuery,
                            Database,
                            query,
                            timingScope,
                            Database.DocumentsStorage,
                            queryContext.Documents,
                            fieldsToFetch,
                            idc,
                            icevc);

                        HashSet<ulong> alreadySeenProjections = null;
                        if (q.IsDistinct)
                        {
                            alreadySeenProjections = new HashSet<ulong>();
                        }
                        foreach (var match in qr.Matches)
                        {
                            if (match.Empty)
                                continue;

                            var result = resultRetriever.ProjectFromMatch(match, queryContext.Documents);
                            // ReSharper disable once PossibleNullReferenceException
                            if (q.IsDistinct && alreadySeenProjections.Add(result.DataHash) == false)
                                continue;
                            final.AddResult(result);
                        }
                    }

                    if (idc == null)
                        idc = new IncludeDocumentsCommand(Database.DocumentsStorage, queryContext.Documents, query.Metadata.Includes, isProjection: false);

                    if (query.Metadata.Includes?.Length > 0)
                    {
                        foreach (var result in final.Results)
                        {
                            idc.Gather(result);
                        }
                    }

                    idc.Fill(final.Includes);

                    final.TotalResults = final.Results.Count;

                    if (query.Limit != null || query.Offset != null)
                    {
                        final.CappedMaxResults = Math.Min(
                            query.Limit ?? int.MaxValue,
                            final.TotalResults - (query.Offset ?? 0)
                            );
                    }

                    final.IsStale = qr.QueryPlan.IsStale;
                    final.ResultEtag = qr.QueryPlan.ResultEtag;
                    return final;
                }
            }
            catch (OperationCanceledException oce)
            {
                throw new OperationCanceledException($"Database:{Database} Query:{query.Metadata.Query} has been cancelled ", oce);
            }
        }

        private void Sort(List<Match> matches, OrderByField[] orderBy, string databaseName, string query)
        {
            if (orderBy.Length == 1)
            {
                var orderByFieldSorter = new GraphQueryOrderByFieldComparer(orderBy.First(), databaseName, query);
                matches.Sort(orderByFieldSorter);
                return;
            }

            var orderByMltipleFieldsSorter = new GraphQueryMultipleFieldsComparer(orderBy, databaseName, query);
            matches.Sort(orderByMltipleFieldsSorter);
        }

        private async Task<(List<Match> Matches, GraphQueryPlan QueryPlan, bool NotModified)> GetQueryResults(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token, bool collectIntermediateResults = false)
        {
            var q = query.Metadata.Query;
            var qp = new GraphQueryPlan(query, queryContext, existingResultEtag, token, Database)
            {
                CollectIntermediateResults = collectIntermediateResults
            };
            qp.BuildQueryPlan();
            qp.OptimizeQueryPlan(); //TODO: audit optimization

            if (query.WaitForNonStaleResults)
            {
                qp.IsStale = await qp.WaitForNonStaleResults();
            }
            else
            {
                await qp.CreateAutoIndexesAndWaitIfNecessary();
            }

            //for the case where we don't wait for non stale results we will override IsStale in the QueryQueryStep steps

            if (queryContext.AreTransactionsOpened() == false)
                queryContext.OpenReadTransaction();

            qp.ResultEtag = DocumentsStorage.ReadLastEtag(queryContext.Documents.Transaction.InnerTransaction);
            if (existingResultEtag.HasValue)
            {
                if (qp.ResultEtag == existingResultEtag)
                    return (null, null, true);
            }
            await qp.Initialize();
            var matchResults = qp.Execute();

            if (query.Metadata.OrderBy != null)
            {
                Sort(matchResults, query.Metadata.OrderBy, Database.Name, query.Query);
            }

            var filter = q.GraphQuery.Where;
            if (filter != null)
            {
                for (int i = 0; i < matchResults.Count; i++)
                {
                    var resultAsJson = new DynamicJsonValue();
                    matchResults[i].PopulateVertices(resultAsJson);

                    using (var result = queryContext.Documents.ReadObject(resultAsJson, "graph/result"))
                    {
                        if (filter.IsMatchedBy(result, query.QueryParameters) == false)
                            matchResults[i] = default;
                    }
                }
            }

            if (query.Start > 0)
            {
                matchResults.RemoveRange(0, Math.Min(query.Start, matchResults.Count));
            }

            if (query.PageSize < matchResults.Count)
            {
                matchResults.RemoveRange(query.PageSize, matchResults.Count - query.PageSize);
            }
            return (matchResults, qp, false);
        }

        private static void HandleResultsWithoutSelect<TResult>(
            DocumentsOperationContext documentsContext,
            List<Match> matchResults, TResult final) where TResult : QueryResultServerSide<Document>
        {
            foreach (var match in matchResults)
            {
                if (match.Empty)
                    continue;

                if (match.Count == 1) //if we don't have multiple results in each row, we can "flatten" the row
                {
                    final.AddResult(match.GetFirstResult());
                    continue;
                }

                var resultAsJson = new DynamicJsonValue();
                match.PopulateVertices(resultAsJson);

                var result = new Document
                {
                    Data = documentsContext.ReadObject(resultAsJson, "graph/result"),
                };

                final.AddResult(result);
            }
        }

        public override async Task ExecuteStreamQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response, IStreamQueryResultWriter<Document> writer, OperationCancelToken token)
        {
            using (var context = QueryOperationContext.Allocate(Database, needsServerContext: false))
            {
                var result = new StreamDocumentQueryResult(response, writer, token)
                {
                    IndexName = Constants.Documents.Indexing.DummyGraphIndexName
                };
                result = await ExecuteQuery(result, query, context, null, token);
                result.Flush();
            }
        }

        public override Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            throw new NotSupportedException("You cannot delete based on graph query");
        }

        public override Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException("Graph queries do not expose index queries");
        }

        public override Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, Patch.PatchRequest patch, BlittableJsonReaderObject patchArgs, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            throw new NotSupportedException("You cannot patch based on graph query");
        }

        public override Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException("You cannot suggest based on graph query");
        }
    }
}
