using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Diagnostics;
using System.Linq;
using System.Security.Policy;
using System.Text;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Utils;
using Sparrow;
using Raven.Server.Json;

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

                var matchResults = ExecutePatternMatch(documentsContext, query, ir) ?? new List<Match>();

                //TODO: handle order by, load, select clauses

                var final = new DocumentQueryResult();

                if (q.Select == null && q.SelectFunctionBody.FunctionText == null)
                {
                    HandleResultsWithoutSelect(documentsContext, matchResults.ToList(), final);
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
            if (matchResults.Count == 1)
            {
                final.AddResult(matchResults[0].GetFirstResult());
                return;
            }

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

        private IEnumerable<Match> ExecutePatternMatch(DocumentsOperationContext documentsContext, IndexQueryServerSide query, IntermediateResults ir)
        {
            var visitor = new GraphExecuteVisitor(ir, query, documentsContext);
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

        private class GraphExecuteVisitor : QueryVisitor
        {
            private readonly IntermediateResults _source;
            private readonly GraphQuery _gq;
            private readonly BlittableJsonReaderObject _queryParameters;
            private readonly DocumentsOperationContext _ctx;

            public IEnumerable<Match> Output => 
                _intermediateOutputs.TryGetValue(_gq.MatchClause, out var results) ? 
                    results : Enumerable.Empty<Match>();

            private readonly Dictionary<QueryExpression,List<Match>> _intermediateOutputs = new Dictionary<QueryExpression, List<Match>>();
            private readonly Dictionary<long,List<Match>> _clauseIntersectionIntermediate = new Dictionary<long, List<Match>>();
            
            private readonly HashSet<string> _includedNodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            private readonly List<Match> _results = new List<Match>();
            private readonly Dictionary<PatternMatchElementExpression,HashSet<StringSegment>> _aliasesInMatch = new Dictionary<PatternMatchElementExpression, HashSet<StringSegment>>();
            
            public GraphExecuteVisitor(IntermediateResults source, IndexQueryServerSide query, DocumentsOperationContext documentsContext)
            {
                _source = source;
                _gq = query.Metadata.Query.GraphQuery;
                _queryParameters = query.QueryParameters;
                _ctx = documentsContext;

            }

            public override void VisitCompoundWhereExpression(BinaryExpression @where)
            {                
                if (!(@where.Left is PatternMatchElementExpression left) || 
                    !(@where.Right is PatternMatchElementExpression right))
                {
                    base.VisitCompoundWhereExpression(@where);
                }
                else
                {
                   
                    VisitExpression(left);
                    VisitExpression(right);

                    switch (where.Operator)
                    {
                        case OperatorType.And:
                            IntersectExpressions(where, left, right);
                            break;
                        case OperatorType.Or:
                            UnionExpressions(where, left, right);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }


            //TODO : make sure there is no double results/invalid permutations of results
            private unsafe void IntersectExpressions(QueryExpression parent,
                PatternMatchElementExpression left, 
                PatternMatchElementExpression right)
            {
                Debug.Assert(_intermediateOutputs.ContainsKey(left));
                Debug.Assert(_intermediateOutputs.ContainsKey(right));

                var aliasIntersection = _aliasesInMatch[left];
                aliasIntersection.IntersectWith(_aliasesInMatch[right]);

                var output = new List<Match>();
                for (int l = 0; l < _intermediateOutputs[left].Count; l++)
                {
                    var leftMatch = _intermediateOutputs[left][l];
                    for (int r = 0; r < _intermediateOutputs[right].Count; r++)
                    {
                        var rightMatch = _intermediateOutputs[right][r];
                        var allIntersectionsMatch = true;
                        foreach (var intersect in aliasIntersection)
                        {
                            if (!leftMatch.TryGetDocPtr(intersect, out var leftPtr) ||
                                !rightMatch.TryGetDocPtr(intersect, out var rightPtr) ||
                                leftPtr != rightPtr)
                            {
                                allIntersectionsMatch = false;
                                break;
                            }
                        }

                        if (allIntersectionsMatch)
                        {
                            var match = new Match();
                            foreach (var mergeAlias in leftMatch.Aliases.Union(rightMatch.Aliases))
                            {
                                var doc = leftMatch.Get(mergeAlias) ?? rightMatch.Get(mergeAlias);
                                Debug.Assert(doc != null);
                                match.TrySet(mergeAlias, doc);
                            }

                            if (match.Count > 0)
                            {
                                output.Add(match);
                            }
                        }

                    }
                }

                _intermediateOutputs.Add(parent, output);
            }

            private void UnionExpressions(QueryExpression parent, PatternMatchElementExpression left, PatternMatchElementExpression right)
            {
                Debug.Assert(_intermediateOutputs.ContainsKey(left));
                Debug.Assert(_intermediateOutputs.ContainsKey(right));

                throw new NotImplementedException();
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

                _intermediateOutputs.Add(ee,new List<Match>());
                var aliases = new HashSet<StringSegment>();
                for (int pathIndex = 1; pathIndex < ee.Path.Length - 1; pathIndex += 2)
                {
                    Debug.Assert(ee.Path[pathIndex].IsEdge);

                    var prevNodeAlias = ee.Path[pathIndex - 1].Alias;
                    var nextNodeAlias = ee.Path[pathIndex + 1].Alias;

                    aliases.Add(prevNodeAlias);
                    aliases.Add(nextNodeAlias);

                    var edge = _gq.WithEdgePredicates[ee.Path[pathIndex].Alias];
                    _gq.WithEdgePredicates[ee.Path[pathIndex].Alias].FromAlias = prevNodeAlias;

                    if (!_source.TryGetByAlias(nextNodeAlias, out var edgeResults))
                        throw new InvalidOperationException("Could not fetch destination nod edge data. This should not happen and is likely a bug.");

                    for (int resultIndex = 0; resultIndex < currentResults.Count; resultIndex++)
                    {
                        var edgeResult = currentResults[resultIndex];
                        var prev = edgeResult.Get(prevNodeAlias);

                        if (TryGetMatches(edge, nextNodeAlias, edgeResults, prev, out var multipleRelatedMatches))
                        {
                            foreach (var match in multipleRelatedMatches)
                            {
                                var related = match.Get(nextNodeAlias);
                                if (edgeResult.TryGetKey(nextNodeAlias, out _) == false)
                                {
                                    edgeResult.Set(nextNodeAlias, related);
                                    //no need to add to Output here, since item is part of currentResults and they will get added later
                                }
                                else
                                {
                                    var multipleEdgeResult = new Match();

                                    multipleEdgeResult.Set(prevNodeAlias, prev);
                                    multipleEdgeResult.Set(nextNodeAlias, related);
                                    _intermediateOutputs[ee].Add(multipleEdgeResult);
                                }
                            }
                            continue;
                        }

                        //if didn't find multiple AND single edges, then it has no place in query results...
                        currentResults.RemoveAt(resultIndex);
                        resultIndex--;
                    }
                }

                _aliasesInMatch.Add(ee,aliases); //if we don't visit each match pattern exactly once, we have an issue 
                _intermediateOutputs[ee].AddRange(currentResults);                
            }

            private void F(BlittableJsonReaderObject docReader, StringSegment edgePath, HashSet<string> includedIds)
            {
                if (BlittableJsonTraverser.Default.TryRead(docReader, edgePath, out object value, out StringSegment leftPath) == false)
                {
                    switch (value)
                    {
                        case BlittableJsonReaderObject json:
                            IncludeUtil.GetDocIdFromInclude(json, leftPath, includedIds);
                            break;
                        case BlittableJsonReaderArray array:
                            foreach (var item in array)
                            {
                                if (item is BlittableJsonReaderObject inner)
                                    IncludeUtil.GetDocIdFromInclude(inner, leftPath, includedIds);
                            }
                            break;
                        case string s:
                            includedIds.Add(s);
                            break;
                        case LazyStringValue lsv:
                            includedIds.Add(lsv.ToString());
                            break;
                        case LazyCompressedStringValue lcsv:
                            includedIds.Add(lcsv.ToString());
                            break;
                        default:
                            break;
                    }

                    return;
                }

            }


            private bool TryGetMatches(WithEdgesExpression edge, string alias, Dictionary<string, Match> edgeResults, Document prev,
                out List<Match> relatedMatches)
            {
                _results.Clear();
                relatedMatches = _results;
                if (edge.Where != null)
                {
                    if (prev.Data.TryGetMember(edge.Path.Compound[0], out var value) == false)
                        return false;

                    bool hasResults = false;

                    switch (value)
                    {
                        case BlittableJsonReaderArray array:
                            foreach (var item in array)
                            {
                                if (item is BlittableJsonReaderObject json &&
                                    edge.Where.IsMatchedBy(json, _queryParameters))
                                {
                                    hasResults |= TryGetMatchesAfterFiltering(json, edge.Path.FieldValueWithoutAlias, edgeResults, alias);
                                }
                            }
                            break;
                        case BlittableJsonReaderObject json:
                            if (edge.Where.IsMatchedBy(json, _queryParameters))
                            {
                                hasResults |= TryGetMatchesAfterFiltering(json, edge.Path.FieldValueWithoutAlias, edgeResults, alias);
                            }
                            break;
                    }

                    return hasResults;

                }
                return TryGetMatchesAfterFiltering(prev.Data, edge.Path.FieldValue, edgeResults, alias);
            }

            private bool TryGetMatchesAfterFiltering(BlittableJsonReaderObject src, string path, Dictionary<string, Match> edgeResults, string alias)
            {
                _includedNodes.Clear();
                IncludeUtil.GetDocIdFromInclude(src,
                   path,
                   _includedNodes);
                _includedNodes.Remove(null);

                if (_includedNodes.Count == 0)
                    return false;

                if (edgeResults == null)
                {
                    foreach (var id in _includedNodes)
                    {

                        var doc = _ctx.DocumentDatabase.DocumentsStorage.Get(_ctx, id, false);
                        if (doc == null)
                            continue;

                        var m = new Match();
                        m.Set(alias, doc);

                        _results.Add(m);
                    }
                }
                else
                {
                    foreach (var id in _includedNodes)
                    {
                        if (id == null)
                            continue;

                        if (!edgeResults.TryGetValue(id, out var m))
                            continue;

                        _results.Add(m);
                    }
                }

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
