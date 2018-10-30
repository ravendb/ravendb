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

        // this code is first draft mode, meant to start working. It is known that 
        // there are LOT of allocations here that we'll need to get under control
        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag,
            OperationCancelToken token)
        {
            using (var timingScope = new QueryTimingsScope())
            {
                var qp = new GraphQueryPlan(query, documentsContext, existingResultEtag, token, Database);
                qp.BuildQueryPlan();
                await qp.Initialize();
                var matchResults = qp.Execute();
                var q = query.Metadata.Query;

                //var q = query.Metadata.Query;
                //var ir = new IntermediateResults();

                //foreach (var documentQuery in q.GraphQuery.WithDocumentQueries)
                //{
                //    var queryMetadata = new QueryMetadata(documentQuery.Value, query.QueryParameters, 0);
                //    if (documentQuery.Value.From.Index)
                //    {
                //        var index = Database.IndexStore.GetIndex(queryMetadata.IndexName);
                //        if (index.Type == IndexType.AutoMapReduce ||
                //            index.Type == IndexType.MapReduce ||
                //            index.Type == IndexType.JavaScriptMapReduce)
                //        {
                //            _mapReduceAliases.Add(documentQuery.Key);
                //        }
                //    }

                //    var indexQuery = new IndexQueryServerSide(queryMetadata);
                //    var results = await Database.QueryRunner.ExecuteQuery(indexQuery, documentsContext, existingResultEtag, token).ConfigureAwait(false);

                //    ir.EnsureExists(documentQuery.Key);

                //    foreach (var result in results.Results)
                //    {
                //        var match = new Match();
                //        match.Set(documentQuery.Key, result);
                //        match.PopulateVertices(ref ir);
                //    }
                //}

                //var matchResults = ExecutePatternMatch(documentsContext, query, ir) ?? new List<Match>();

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

                //TODO: handle order by, load, include clauses

                var final = new DocumentQueryResult();

                if (q.Select == null && q.SelectFunctionBody.FunctionText == null)
                {
                    HandleResultsWithoutSelect(documentsContext, matchResults, final);
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



                    foreach (var match in matchResults)
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


        private static void HandleResultsWithoutSelect(
            DocumentsOperationContext documentsContext,
            List<Match> matchResults, DocumentQueryResult final)
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

        private List<Match> ExecutePatternMatch(DocumentsOperationContext documentsContext, IndexQueryServerSide query, IntermediateResults ir)
        {
            var visitor = new GraphExecuteVisitor(ir, query, documentsContext, _mapReduceAliases);
            visitor.VisitExpression(query.Metadata.Query.GraphQuery.MatchClause);
            return visitor.Output;
        }

        public override Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamDocumentQueryResultWriter writer, OperationCancelToken token)
        {
            throw new NotImplementedException("Streaming graph queries is not supported at this time");
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
