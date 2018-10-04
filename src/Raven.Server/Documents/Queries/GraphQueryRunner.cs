using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Client;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Queries
{
    public partial class GraphQueryRunner : AbstractQueryRunner
    {
        public GraphQueryRunner(DocumentDatabase database) : base(database)
        {
        }

        // this code is first draft mode, meant to start working. It is known that 
        // there are LOT of allocations here that we'll need to get under control
        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag,
            OperationCancelToken token)
        {
            var q = query.Metadata.Query;

            using (var timingScope = new QueryTimingsScope())
            {
                var ir = new IntermediateResults();

                foreach (var documentQuery in q.GraphQuery.WithDocumentQueries)
                {
                    var queryMetadata = new QueryMetadata(documentQuery.Value, query.QueryParameters, 0);
                    var results = await Database.QueryRunner.ExecuteQuery(new IndexQueryServerSide(queryMetadata),
                        documentsContext, existingResultEtag, token);

                    ir.EnsureExists(documentQuery.Key);

                    foreach (var result in results.Results)
                    {
                        var match = new Match();
                        match.Set(documentQuery.Key, result);
                        match.PopulateVertices(ref ir);
                    }
                }

                var matchResults = ExecutePatternMatch(documentsContext, q, query.Metadata, ir) ?? new List<Match>();

                //TODO: handle order by, load, select clauses

                var final = new DocumentQueryResult();

                if (q.Select == null && q.SelectFunctionBody.FunctionText == null)
                {
                    HandleResultsWithoutSelect(documentsContext, matchResults, final);
                }
                else if (q.Select != null)
                {
                    var fieldsToFetch = new FieldsToFetch(query.Metadata.SelectFields,null);
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
                        var json = new DynamicJsonValue();
                        match.Populate(json, query.Metadata.AliasesInGraphSelect, q.GraphQuery);

                        var doc = documentsContext.ReadObject(json, "graph/projection");
                        var projectedDoc = resultRetriever.Get(new Document
                        {
                            Data = doc
                        });
                     
                        final.AddResult(projectedDoc);
                    }
                }        

                final.TotalResults = final.Results.Count;
                return final;
            }
        }

        private static void HandleResultsWithoutSelect(DocumentsOperationContext documentsContext, List<Match> matchResults, DocumentQueryResult final)
        {
            foreach (var match in matchResults)
            {
                var resultAsJson = new DynamicJsonValue();
                match.PopulateVertices(resultAsJson);

                var result = new Document
                {
                    Data = documentsContext.ReadObject(resultAsJson, "graph/result"),
                };

                final.AddResult(result);
            }
        }

        private List<Match> ExecutePatternMatch(DocumentsOperationContext documentsContext, Query q, QueryMetadata queryMetadata, IntermediateResults ir)
        {
            var visitor = new GraphExecuteVisitor(ir, q.GraphQuery, documentsContext);
            visitor.VisitExpression(q.GraphQuery.MatchClause);
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

        private class GraphExecuteVisitor : QueryVisitor
        {
            private readonly IntermediateResults _source;
            private readonly GraphQuery _gq;
            private readonly DocumentsOperationContext _ctx;
            public List<Match> Output;            

            public GraphExecuteVisitor(IntermediateResults source, GraphQuery gq, DocumentsOperationContext documentsContext)
            {
                _source = source;
                _gq = gq;
                _ctx = documentsContext;
            }

            public override void VisitPatternMatchElementExpression(PatternMatchElementExpression ee)
            {                
                Debug.Assert(ee.Path[0].EdgeType == EdgeType.Right);
                if (_source.TryGetByAlias(ee.Path[0].Alias, out var nodeResults) == false || 
                    nodeResults.Count == 0)
                    return; // if root is empty, the entire thing is empty
                
                var currentResults = new List<Match>();
                foreach (var item in nodeResults)
                {
                    var match = new Match();
                    match.Set(ee.Path[0].Alias, item.Value.Get(ee.Path[0].Alias));
                    currentResults.Add(match);
                }
                
                Output = new List<Match>();

                // TODO: for now, we require node->edge->node->edge syntax
                for (int pathIndex = 1; pathIndex < ee.Path.Length-1; pathIndex+=2)
                {
                    Debug.Assert(ee.Path[pathIndex].IsEdge);
                    
                    var prevNodeAlias = ee.Path[pathIndex - 1].Alias;
                    var nextNodeAlias = ee.Path[pathIndex + 1].Alias;
                    
                    var edge = _gq.WithEdgePredicates[ee.Path[pathIndex].Alias].EdgeType.Value;
                    _gq.WithEdgePredicates[ee.Path[pathIndex].Alias].FromAlias = prevNodeAlias;

                    if(!_source.TryGetByAlias(nextNodeAlias, out var edgeResults))
                        throw new InvalidOperationException("Could not fetch destination nod edge data. This should not happen and is likely a bug.");

                    for (int resultIndex = 0; resultIndex < currentResults.Count; resultIndex++)
                    {
                        var edgeResult = currentResults[resultIndex];
                        var prev = edgeResult.Get(prevNodeAlias);
                        //for cases when there are mulitple possible edges for one vertex
                        //something like { RelatedProducts : ["products/1", "products/3"]
                        Document related;
                        if (TryGetMultipleRelatedMatches(edge, nextNodeAlias, edgeResults, prev, out var multipleRelatedMatches))
                        {
                            foreach (var match in multipleRelatedMatches)
                            {
                                related = match.Get(nextNodeAlias);
                                if (edgeResult.TryGetKey(nextNodeAlias,out _) == false)
                                {
                                    edgeResult.Set(nextNodeAlias,related); 
                                    //no need to add to Output here, since item is part of currentResults and they will get added later
                                }
                                else
                                {
                                    var multipleEdgeResult = new Match();
                                    
                                    multipleEdgeResult.Set(prevNodeAlias, prev);
                                    multipleEdgeResult.Set(nextNodeAlias, related);
                                    Output.Add(multipleEdgeResult);
                                }
                            }
                            continue;
                        }

                        if (!TryGetRelatedMatch(edge, nextNodeAlias, edgeResults, prev, out var relatedMatch))
                        {
                            //if didn't find multiple AND single edges, then it has no place in query results...
                            currentResults.RemoveAt(resultIndex);
                            resultIndex--;
                            continue;
                        }

                        related = relatedMatch.Get(nextNodeAlias);
                        edgeResult.Set(nextNodeAlias, related);
                    }
                }

                Output.AddRange(currentResults);
            }

            private bool TryGetMultipleRelatedMatches(string edge, string alias, Dictionary<string, Match> edgeResults, Document prev, out IEnumerable<Match> relatedMatches)
            {
                var nextIds = new HashSet<string>();
                IncludeUtil.GetDocIdFromInclude(prev.Data,edge,nextIds);
                relatedMatches = Enumerable.Empty<Match>();
                if (prev.Data.TryGet(edge, out string[] nextIds) == false || nextIds == null)
                    return false;

                IEnumerable<Match> GetResultsFromEdges()
                {
                    foreach (var id in nextIds)
                    {
                  
                        if(!edgeResults.TryGetValue(id,out var m))
                            continue;
                        yield return m;
                    }
                }

                IEnumerable<Match> GetResultsFromStorage()
                {
                    foreach (var id in nextIds)
                    {

                        var doc = _ctx.DocumentDatabase.DocumentsStorage.Get(_ctx, id, false);
                        if (doc == null)
                            continue;

                        var m = new Match();
                        m.Set(alias, doc);

                        yield return m;
                    }
                }

                relatedMatches = edgeResults != null ? GetResultsFromEdges() : GetResultsFromStorage();

                return true;
            }

            private bool TryGetRelatedMatch(string edge, string alias, Dictionary<string, Match> edgeResults, Document prev, out Match relatedMatch)
            {
                relatedMatch = default;
                if (prev.Data.TryGet(edge, out string nextId) == false || nextId == null)
                    return false;

                if (edgeResults != null)
                {
                    return edgeResults.TryGetValue(nextId, out relatedMatch);
                }

                var doc = _ctx.DocumentDatabase.DocumentsStorage.Get(_ctx, nextId, false);
                if (doc == null)
                    return false;

                relatedMatch = new Match();
                relatedMatch.Set(alias, doc);
                return true;
            }
        }
    }
}
