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
                if (!(@where.Left is PatternMatchElementExpression left))
                {
                    base.VisitCompoundWhereExpression(@where);
                }               
                else
                {
                    
                    VisitExpression(left);
                    VisitExpression(@where.Right);

                    switch (where.Operator)
                    {
                        case OperatorType.And:
                            if (@where.Right is NegatedExpression n)
                            {
                                IntersectExpressions<Except>(where, left, (PatternMatchElementExpression)n.Expression);
                            }
                            else
                            {
                                IntersectExpressions<Intersection>(where, left, (PatternMatchElementExpression)@where.Right);
                            }
                            break;
                        case OperatorType.Or:
                            IntersectExpressions<Union>(where, left, (PatternMatchElementExpression)@where.Right);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }


            //TODO : make sure there is no double results/invalid permutations of results

            private Dictionary<long, List<Match>> _tempIntersect = new Dictionary<long, List<Match>>();

            private interface ISetOp
            {
                void Op(List<Match> output, 
                    (Match Match, HashSet<StringSegment> Aliases) left,
                    (Match Match, HashSet<StringSegment> Aliases) right, 
                    bool allIntersectionsMatch,
                    HashSet<Match> state);

                bool CanOptimizeSides { get; }
                bool ShouldContinueWhenNoIntersection { get; }
                void Complete(List<Match> output, Dictionary<long, List<Match>>intersection, HashSet<StringSegment> aliases, HashSet<Match> state);
            }

            private struct Intersection : ISetOp
            {
                public bool CanOptimizeSides => true;
                public bool ShouldContinueWhenNoIntersection => false;

                public void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<StringSegment> aliases, HashSet<Match> state)
                {
                    // nothing to do
                }

                public void Op(List<Match> output,
                    (Match Match, HashSet<StringSegment> Aliases) left,
                    (Match Match, HashSet<StringSegment> Aliases) right,
                    bool allIntersectionsMatch,
                    HashSet<Match> state)
                {
                    if (allIntersectionsMatch == false)
                        return;

                    var resultMatch = new Match();

                    CopyAliases(left.Match, ref resultMatch, left.Aliases);
                    CopyAliases(right.Match, ref resultMatch, right.Aliases);
                    output.Add(resultMatch);
                }
            }

            private struct Union : ISetOp
            {
                public bool CanOptimizeSides => true;
                public bool ShouldContinueWhenNoIntersection => true;

                public void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<StringSegment> aliases, HashSet<Match> state)
                {
                    foreach (var  kvp in intersection)
                    {
                        foreach (var item in kvp.Value)
                        {
                            if (state.Contains(item) == false)
                            {
                                output.Add(item);
                            }
                        }
                    }

                    foreach(var nonIntersectedItem in state)
                        output.Add(nonIntersectedItem);
                }

                public void Op(List<Match> output,
                    (Match Match, HashSet<StringSegment> Aliases) left,
                    (Match Match, HashSet<StringSegment> Aliases) right,
                    bool allIntersectionsMatch,
                    HashSet<Match> state)
                {
                    if (allIntersectionsMatch == false)
                    {
                        output.Add(right.Match);
                        return;
                    }

                    var resultMatch = new Match();

                    CopyAliases(left.Match, ref resultMatch, left.Aliases);
                    CopyAliases(right.Match, ref resultMatch, right.Aliases);
                    output.Add(resultMatch);
                    state.Add(left.Match);
                }
            }

            private struct Except : ISetOp
            {
                // for AND NOT, the sides really matter, so we can't optimize it
                public bool CanOptimizeSides => false;
                public bool ShouldContinueWhenNoIntersection => true;

                public void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<StringSegment> aliases, HashSet<Match> state)
                {
                    foreach (var kvp in intersection)
                    {
                        foreach (var item in kvp.Value)
                        {
                            if (state.Contains(item) == false)
                                output.Add(item);
                        }
                    }
                }

                public void Op(List<Match> output,
                    (Match Match, HashSet<StringSegment> Aliases) left,
                    (Match Match, HashSet<StringSegment> Aliases) right,
                    bool allIntersectionsMatch,
                    HashSet<Match> state)
                {
                    if (allIntersectionsMatch)
                        state.Add(left.Match);
                }
            }

            private void IntersectExpressions<TOp>(QueryExpression parent,
                PatternMatchElementExpression left, 
                PatternMatchElementExpression right)
                where TOp : struct, ISetOp
            {
                _tempIntersect.Clear();

                var operation = new TOp();
                var operationState = new HashSet<Match>();
                // TODO: Move this to the parent object
                var intersectedAliases = _aliasesInMatch[left].Intersect(_aliasesInMatch[right]).ToList();

                if (intersectedAliases.Count == 0 && !operation.ShouldContinueWhenNoIntersection)
                    return; // no matching aliases, so we need to stop when the operation is intersection

                var xOutput = _intermediateOutputs[left];
                var xAliases = _aliasesInMatch[left];
                var yOutput = _intermediateOutputs[right];
                var yAliases = _aliasesInMatch[right];

                // ensure that we start processing from the smaller side
                if(xOutput.Count < yOutput.Count && operation.CanOptimizeSides)
                {
                    var tmp = xOutput;
                    yOutput = xOutput;
                    xOutput = tmp;
                    var tmpAliases = xAliases;
                    xAliases = yAliases;
                    yAliases = tmpAliases;
                }

                for (int l = 0; l < xOutput.Count; l++)
                {
                    var xMatch = xOutput[l];
                    long key = GetMatchHashKey(intersectedAliases, xMatch);

                    if (_tempIntersect.TryGetValue(key, out var matches) == false)
                        _tempIntersect[key] = matches = new List<Match>(); // TODO: pool these
                    matches.Add(xMatch);
                }

                var output = new List<Match>();

                for (int l = 0; l < yOutput.Count; l++)
                {
                    var yMatch = yOutput[l];
                    long key = GetMatchHashKey(intersectedAliases, yMatch);

                    if (_tempIntersect.TryGetValue(key, out var matchesFromLeft) == false)
                    {
                        if (operation.ShouldContinueWhenNoIntersection)
                            operationState.Add(yMatch);
                        continue; // nothing matched, can skip
                    }

                    for (int i = 0; i < matchesFromLeft.Count; i++)
                    {
                        var xMatch = matchesFromLeft[i];
                        var allIntersectionsMatch = true;
                        for (int j = 0; j < intersectedAliases.Count; j++)
                        {
                            var intersect = intersectedAliases[j];
                            if (!xMatch.TryGetAliasId(intersect, out var x) ||
                                !yMatch.TryGetAliasId(intersect, out var y) ||
                                x != y)
                            {
                                allIntersectionsMatch = false;
                                break;
                            }
                        }

                        operation.Op(output, (xMatch, xAliases), (yMatch, yAliases), allIntersectionsMatch, operationState);
                    }
                }

                operation.Complete(output, _tempIntersect, xAliases, operationState);

                _intermediateOutputs.Add(parent, output);
            }

            private static void CopyAliases(Match src, ref Match dst, HashSet<StringSegment> aliases)
            {
                foreach (var alias in aliases)
                {
                    var doc = src.Get(alias);
                    if(doc == null)
                        continue;
                    dst.TrySet(alias, doc);
                }
            }

            private static long GetMatchHashKey(List<StringSegment> intersectedAliases, Match match)
            {
                long key = 0L;
                for (int i = 0; i < intersectedAliases.Count; i++)
                {
                    var alias = intersectedAliases[i];

                    if (match.TryGetAliasId(alias, out long aliasId) == false)
                        aliasId = -i;

                    key = Hashing.Combine(key, aliasId);
                }

                return key;
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
