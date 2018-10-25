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
using System.Security.Policy;
using System.Text;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections.LockFree;

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
            var q = query.Metadata.Query;
            
            using (var timingScope = new QueryTimingsScope())
            {
                var ir = new IntermediateResults();

                foreach (var documentQuery in q.GraphQuery.WithDocumentQueries)
                {
                    var queryMetadata = new QueryMetadata(documentQuery.Value, query.QueryParameters, 0);
                    if (documentQuery.Value.From.Index)
                    {
                        var index = Database.IndexStore.GetIndex(queryMetadata.IndexName);
                        if (index.Type == IndexType.AutoMapReduce ||
                            index.Type == IndexType.MapReduce ||
                            index.Type == IndexType.JavaScriptMapReduce)
                        {
                            _mapReduceAliases.Add(documentQuery.Key);
                        }
                    }

                    var indexQuery = new IndexQueryServerSide(queryMetadata);
                    var results = await Database.QueryRunner.ExecuteQuery(indexQuery, documentsContext, existingResultEtag, token).ConfigureAwait(false);

                    ir.EnsureExists(documentQuery.Key);

                    foreach (var result in results.Results)
                    {
                        var match = new Match();
                        match.Set(documentQuery.Key, result);
                        match.PopulateVertices(ref ir);
                    }
                }

                var matchResults = ExecutePatternMatch(documentsContext, query, ir) ?? new List<Match>();

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

        private class GraphExecuteVisitor : QueryVisitor
        {
            private readonly IntermediateResults _source;
            private readonly GraphQuery _gq;
            private readonly BlittableJsonReaderObject _queryParameters;
            private readonly DocumentsOperationContext _ctx;
            private readonly HashSet<StringSegment> _mapReduceAliases;

            private static List<Match> Empty = new List<Match>();

            public List<Match> Output => 
                _intermediateOutputs.TryGetValue(_gq.MatchClause, out var results) ? 
                    results : Empty;

            private readonly Dictionary<QueryExpression,List<Match>> _intermediateOutputs = new Dictionary<QueryExpression, List<Match>>();
            private readonly Dictionary<long,List<Match>> _clauseIntersectionIntermediate = new Dictionary<long, List<Match>>();
            
            private readonly Dictionary<string, Document> _includedEdges = new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
            private readonly List<Match> _results = new List<Match>();
            private readonly Dictionary<PatternMatchElementExpression,HashSet<StringSegment>> _aliasesInMatch = new Dictionary<PatternMatchElementExpression, HashSet<StringSegment>>();
            
            public GraphExecuteVisitor(IntermediateResults source, IndexQueryServerSide query, DocumentsOperationContext documentsContext,
                HashSet<StringSegment> mapReduceAliases)
            {
                _source = source;
                _gq = query.Metadata.Query.GraphQuery;
                _queryParameters = query.QueryParameters;
                _ctx = documentsContext;
                _mapReduceAliases = mapReduceAliases;
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
                           if (@where.Right is NegatedExpression n &&
                               n.Expression is PatternMatchElementExpression rightNegatedPatternMatch)
                           {
                                IntersectExpressions<Except>(where, left, rightNegatedPatternMatch);
                           }
                           else if(@where.Right is PatternMatchElementExpression right)
                           {
                                IntersectExpressions<Intersection>(where, left, right);
                           }
                           else
                           {
                                throw new InvalidQueryException($"Failed to execute graph query because found unexpected right clause expression type. Expected it to be either {nameof(NegatedExpression)} or {nameof(PatternMatchElementExpression)} but found expression type = {@where.Right.GetType().FullName}");
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

            private unsafe void IntersectExpressions<TOp>(QueryExpression parent,
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
                    var tmp = yOutput;
                    yOutput = xOutput;
                    xOutput = tmp;
                    var tmpAliases = yAliases;
                    yAliases = xAliases;
                    xAliases = tmpAliases;
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
                    var doc = src.GetSingleDocumentResult(alias);
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
                if (_source.TryGetMatchesForAlias(ee.Path[0].Alias, out var nodeResults) == false ||
                    nodeResults.Count == 0)
                {
                    _intermediateOutputs.Add(ee,new List<Match>());
                    _aliasesInMatch.Add(ee, new HashSet<StringSegment>());
                    return; // if root is empty, the entire thing is empty
                }

                var currentResults = new List<Match>();
                foreach (var item in nodeResults)
                {
                    var match = new Match();
                    match.Set(ee.Path[0].Alias, item.GetSingleDocumentResult(ee.Path[0].Alias));
                    currentResults.Add(match);
                }
                
                _intermediateOutputs.Add(ee,new List<Match>());
                var aliases = new HashSet<StringSegment>();
                for (int pathIndex = 1; pathIndex < ee.Path.Length-1; pathIndex+=2)
                {
                    Debug.Assert(ee.Path[pathIndex].IsEdge);

                    var prevNodeAlias = ee.Path[pathIndex - 1].Alias;
                    var nextNodeAlias = ee.Path[pathIndex + 1].Alias;
                    if (_mapReduceAliases.Contains(nextNodeAlias))
                    {
                        throw new InvalidOperationException("Target vertices in a pattern match that originate from map/reduce WITH clause are not allowed. (pattern match has multiple statements in the form of (a)-[:edge]->(b) ==> in such pattern, 'b' must not originate from map/reduce index query)");
                    }
                    var edgeAlias = ee.Path[pathIndex].Alias;
                    var edge = _gq.WithEdgePredicates[edgeAlias];
                    edge.EdgeAlias = edgeAlias;
                    edge.FromAlias = prevNodeAlias;

                    aliases.Add(prevNodeAlias);
                    aliases.Add(nextNodeAlias);

                    if (!_source.TryGetByAlias(nextNodeAlias, out var edgeResults))
                        throw new InvalidOperationException("Could not fetch destination nod edge data. This should not happen and is likely a bug.");

                    AddToResultsIfMatch(currentResults, prevNodeAlias, nextNodeAlias, edgeAlias, edge, edgeResults);
                }

                _aliasesInMatch.Add(ee,aliases); //if we don't visit each match pattern exactly once, we have an issue 

                var listMatches = _intermediateOutputs[ee];
                foreach (var item in currentResults)
                {
                    if (item.Empty == false)
                        listMatches.Add(item);
                }
            }

            private void AddToResultsIfMatch(
                List<Match> currentResults, 
                StringSegment prevNodeAlias, 
                StringSegment nextNodeAlias, 
                StringSegment edgeAlias, 
                WithEdgesExpression edge, 
                Dictionary<string, Match> edgeResults)
            {
                var currentResultsStartingSize = currentResults.Count;
                for (int resultIndex = 0; resultIndex < currentResultsStartingSize; resultIndex++)
                {
                    var edgeResult = currentResults[resultIndex];

                    if (edgeResult.Empty)
                        continue;

                    var prev = edgeResult.GetSingleDocumentResult(prevNodeAlias);

                    if (TryGetMatches(edge, nextNodeAlias, edgeResults, prev, out var multipleRelatedMatches))
                    {
                        bool reusedSlot = false;
                        foreach (var match in multipleRelatedMatches)
                        {
                            var related = match.GetSingleDocumentResult(nextNodeAlias);
                            var relatedEdge = match.GetResult(edgeAlias);
                            var updatedMatch = new Match(edgeResult);

                            updatedMatch.Set(edgeAlias, relatedEdge);
                            updatedMatch.Set(nextNodeAlias, related);

                            if (reusedSlot)
                            {
                                currentResults.Add(updatedMatch);
                            }
                            else
                            {
                                reusedSlot = true;
                                currentResults[resultIndex] = updatedMatch;
                            }

                        }
                        continue;
                    }

                    //if didn't find multiple AND single edges, then it has no place in query results...
                    currentResults[resultIndex] = default;
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
                                if(item is BlittableJsonReaderObject json &&
                                    edge.Where.IsMatchedBy(json, _queryParameters))
                                {
                                    hasResults |= TryGetMatchesAfterFiltering(json, edge.Path.FieldValueWithoutAlias, edgeResults, alias, edge.EdgeAlias, _results);
                                }
                            }
                            break;
                        case BlittableJsonReaderObject json:
                            if (edge.Where.IsMatchedBy(json, _queryParameters))
                            {
                                hasResults |= TryGetMatchesAfterFiltering(json, edge.Path.FieldValueWithoutAlias, edgeResults, alias, edge.EdgeAlias, _results);
                            }
                            break;
                    }

                    return hasResults;

                }
                if(edge.VariableLength != null)
                    return TryGetVariableLengthMatchesAfterFiltering(prev.Data, edge, edgeResults, alias);

                return TryGetMatchesAfterFiltering(prev.Data, edge.Path.FieldValue, edgeResults, alias, edge.EdgeAlias, _results);
            }

             private struct IncludeEdgeOp : IncludeUtil.IIncludeOp
            {
                 GraphExecuteVisitor _parent;

                public IncludeEdgeOp(GraphExecuteVisitor parent)
                {
                    _parent = parent;
                }

                public void Include(BlittableJsonReaderObject edge, string id)
                {
                    if (id == null)
                        return;
                    _parent._includedEdges[id] = edge == null ? null : new Document
                    {
                        Data = edge,
                    };
                }
            }

            private bool TryGetVariableLengthMatchesAfterFiltering(BlittableJsonReaderObject src, WithEdgesExpression edge, Dictionary<string, Match> edgeResults, string docAlias)
            {
                var min = edge.VariableLength.Value.Min ?? 0;
                var max = edge.VariableLength.Value.Max ?? int.MaxValue;
            
                var pathMembers = new Stack<(BlittableJsonReaderObject Src, List<Match> Matches)>();
                var cur = src;
                Match currentMatch = default;
                bool hasResults = false;
                while(true)
                {
                    var tmp = new List<Match>();
                    if (pathMembers.Count + 1 == max)// max path we can handle
                    {
                        AddMatch();
                    }
                    else
                    {
                        if (TryGetMatchesAfterFiltering(cur, edge.Path.FieldValue,edgeResults, docAlias, edge.EdgeAlias, tmp) == false)
                        {
                            if (min <= pathMembers.Count)
                            {
                                AddMatch();
                            }
                        }
                        else
                        {
                            pathMembers.Push((cur, tmp));
                        }
                    }

                    while (true)
                    {
                        if (pathMembers.Count == 0)
                            return hasResults;

                        var top = pathMembers.Peek();
                        if (top.Matches.Count == 0)
                        {
                            pathMembers.Pop();
                            continue;
                        }
                        currentMatch = top.Matches[top.Matches.Count - 1];
                        cur = currentMatch.GetSingleDocumentResult(docAlias).Data;
                        top.Matches.RemoveAt(top.Matches.Count - 1);
                        break;
                    }
                } 

                void AddMatch()
                {
                    hasResults = true;
                    var list = new List<Document>(pathMembers.Count-1);
                    foreach (var item in pathMembers)
                    {
                        list.Add(new Document { Data = item.Src });
                        if (list.Count == list.Capacity)
                            break;// s
                    }
                    list.Reverse();
                    var match = new Match(currentMatch);
                    match.ForceSet(edge.EdgeAlias, list);
                    _results.Add(match);
                }
            }


            private bool TryGetMatchesAfterFiltering(
                BlittableJsonReaderObject src, 
                string path, 
                Dictionary<string, Match> edgeResults, 
                string docAlias, 
                string edgeAlias, 
                List<Match> results)
            {
                _includedEdges.Clear();
                var op = new IncludeEdgeOp(this);
                IncludeUtil.GetDocIdFromInclude(src,
                   path,
                   op);


                if (_includedEdges.Count == 0)
                    return false;

                if (edgeResults == null)
                {
                    foreach (var kvp in _includedEdges)
                    {
                        var doc = _ctx.DocumentDatabase.DocumentsStorage.Get(_ctx, kvp.Key, false);
                        if (doc == null)
                            continue;

                        var m = new Match();

                        m.Set(docAlias, doc);
                        if (kvp.Value != null)
                            m.Set(edgeAlias, kvp.Value);

                        results.Add(m);
                    }
                }
                else
                {
                    foreach (var kvp in _includedEdges)
                    {

                        if (kvp.Key == null)
                            continue;

                        if (!edgeResults.TryGetValue(kvp.Key, out var m))
                            continue;

                        var clone = new Match(m);

                        if (kvp.Value != null)
                            clone.Set(edgeAlias, kvp.Value);

                        results.Add(clone);
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
