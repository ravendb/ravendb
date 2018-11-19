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

        public class GraphDebugInfo
        {
            private readonly DocumentDatabase _database;
            private readonly DocumentsOperationContext _context;
            public Dictionary<string, object> Nodes = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            public Dictionary<string, Dictionary<object, HashSet<string>>> Edges = new Dictionary<string, Dictionary<object, HashSet<string>>>();

            public GraphDebugInfo(DocumentDatabase database, DocumentsOperationContext context)
            {
                _database = database;
                _context = context;
            }

            public void AddEdge(string edge, object src, string dst)
            {
                if (src == null || dst == null)
                    return;

                if (Edges.TryGetValue(edge, out var edgeInfo) == false)
                    Edges[edge] = edgeInfo = new Dictionary<object, HashSet<string>>();

                if (edgeInfo.TryGetValue(src, out var hash) == false)
                    edgeInfo[src] = hash = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                hash.Add(dst);

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

                //TODO: handle order by, load, include clauses


                if (q.Select == null && q.SelectFunctionBody.FunctionText == null)
                {
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
                }

                final.TotalResults = final.Results.Count;
                return final;
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

            return (matchResults, qp);
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
