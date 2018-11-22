using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Graph;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
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


        public async Task<GraphDebugInfo> GetAnalyzedQueryResults(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag,
           OperationCancelToken token)
        {
            var qr = await GetQueryResults(query, documentsContext, existingResultEtag, token);
            var result = new GraphDebugInfo(Database, documentsContext);
            qr.QueryPlan.Analyze(qr.Matches, result);
            return result;
        }

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag,
            OperationCancelToken token)
        {
            var res = new DocumentQueryResult
            {
                IndexName = "@graph"
            };
            return await ExecuteQuery(res, query, documentsContext, existingResultEtag, token);
        }

        private async Task<TResult> ExecuteQuery<TResult>(TResult final,IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token) where TResult : QueryResultServerSide
        {
            using (var timingScope = new QueryTimingsScope())
            {
                var qr = await GetQueryResults(query, documentsContext, existingResultEtag, token);
                var q = query.Metadata.Query;

                //TODO: handle order by, load,  clauses
                if (query.Metadata.OrderBy != null)
                {
                    Sort(qr, query.Metadata.OrderBy);
                }

                if (q.Select == null && q.SelectFunctionBody.FunctionText == null)
                {
                    // include clause
                    HandleResultsWithoutSelect(documentsContext, qr.Matches, final);
                }
                else if (q.Select != null)
                {
                    var fieldsToFetch = new FieldsToFetch(query.Metadata.SelectFields, null);
                    var resultRetriever = new GraphQueryResultRetriever(
                        q.GraphQuery,
                        Database,
                        query,
                        timingScope,
                        Database.DocumentsStorage,
                        documentsContext,
                        fieldsToFetch, null);


                    foreach (var match in qr.Matches)
                    {
                        if (match.Empty)
                            continue;

                        var result = resultRetriever.ProjectFromMatch(match, documentsContext);

                        final.AddResult(result);
                    }

                    //include clause
                }

                final.TotalResults = final.Results.Count;
                return final;
            }
        }

        private void Sort((List<Match> Matches, GraphQueryPlan QueryPlan) qr, OrderByField[] orderBy)
        {
            foreach (var field in orderBy)
            {
                var orderByFieldSorter = new GraphQueryOrderByFieldSorter(field);
                qr.Matches.Sort(orderByFieldSorter);
            }
        }


        private async Task<(List<Match> Matches, GraphQueryPlan QueryPlan)> GetQueryResults(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            var q = query.Metadata.Query;
            var qp = new GraphQueryPlan(query, documentsContext, existingResultEtag, token, Database);
            qp.BuildQueryPlan();
            qp.OptimizeQueryPlan(); //TODO: audit optimization
            await qp.Initialize();            
            var matchResults = qp.Execute();

            var filter = q.GraphQuery.Where;
            if (filter != null)
            {
                for (int i = 0; i < matchResults.Count; i++)
                {
                    var resultAsJson = new DynamicJsonValue();
                    matchResults[i].PopulateVertices(resultAsJson);

                    using (var result = documentsContext.ReadObject(resultAsJson, "graph/result"))
                    {
                        if (filter.IsMatchedBy(result, query.QueryParameters) == false)
                            matchResults[i] = default;
                    }
                }
            }            
            return (matchResults.Skip(query.Start).Take(query.PageSize).ToList(), qp);
        }

        private static void HandleResultsWithoutSelect<TResult>(
            DocumentsOperationContext documentsContext,
            List<Match> matchResults, TResult final) where TResult : QueryResultServerSide
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

        public override async Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamDocumentQueryResultWriter writer, OperationCancelToken token)
        {
            var result = new StreamDocumentQueryResult(response, writer, token)
            {
                IndexName = "@graph"
            };
            result =  await ExecuteQuery(result, query, documentsContext, null, token);
            result.Flush();
        }

        public override Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            throw new NotSupportedException("You cannot delete based on graph query");
        }

        public override Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException("Graph queries do not expose index queries");
        }

        public override Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, Patch.PatchRequest patch, BlittableJsonReaderObject patchArgs, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            throw new NotSupportedException("You cannot patch based on graph query");
        }

        public override Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException("You cannot suggest based on graph query");
        }

    }
}
