using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public partial class GraphQueryRunner
    {
        private partial class GraphExecuteVisitor : QueryVisitor
        {
            private readonly IntermediateResults _source;
            private IndexQueryServerSide _query;
            private readonly GraphQuery _gq;
            private readonly BlittableJsonReaderObject _queryParameters;
            private readonly DocumentsOperationContext _ctx;
            private readonly HashSet<StringSegment> _mapReduceAliases;

            private static readonly List<Match> Empty = new List<Match>();

            public List<Match> Output =>
                _intermediateOutputs.TryGetValue(_gq.MatchClause, out var results) ?
                    results : Empty;

            private readonly Dictionary<QueryExpression, List<Match>> _intermediateOutputs = new Dictionary<QueryExpression, List<Match>>();
            private readonly Dictionary<long, List<Match>> _clauseIntersectionIntermediate = new Dictionary<long, List<Match>>();

            private readonly Dictionary<string, Document> _includedEdges = new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
            private readonly List<Match> _results = new List<Match>();
            private readonly Dictionary<PatternMatchElementExpression, HashSet<StringSegment>> _aliasesInMatch = new Dictionary<PatternMatchElementExpression, HashSet<StringSegment>>();

            public GraphExecuteVisitor(IntermediateResults source, IndexQueryServerSide query, DocumentsOperationContext documentsContext,
                HashSet<StringSegment> mapReduceAliases)
            {
                _source = source;
                _query = query;
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
                            else if (@where.Right is PatternMatchElementExpression right)
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

            private static void CopyAliases(Match src, ref Match dst, HashSet<StringSegment> aliases)
            {
                foreach (var alias in aliases)
                {
                    var doc = src.GetSingleDocumentResult(alias);
                    if (doc == null)
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
                    _intermediateOutputs.Add(ee, new List<Match>());
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

                _intermediateOutputs.Add(ee, new List<Match>());
                var aliases = new HashSet<StringSegment>();
                int pathIndex = 1;
                while (pathIndex < ee.Path.Length)
                {
                    var matchPath = ee.Path[pathIndex];
                    Debug.Assert(matchPath.IsEdge);

                    var prevNodeAlias = ee.Path[pathIndex - 1].Alias;
                  
                    if (matchPath.Recursive == null)
                    {
                        var nextNodeAlias = ee.Path[pathIndex + 1].Alias;
                        EnsureValidNextAlias(nextNodeAlias);

                        ProcessSingleMatchPart(currentResults, aliases, matchPath, prevNodeAlias, nextNodeAlias, new DirectExtrator());
                        pathIndex += 2;
                    }
                    else
                    {
                        var pattern = matchPath.Recursive.Value.Pattern;
                        StringSegment nextNodeAlias;
                        var atEnd = pathIndex + 1 == ee.Path.Length;
                        if (atEnd)
                        {
                            if(pattern[pattern.Count - 1].IsEdge)
                                throw new InvalidOperationException("Recursive expression that is the last element in the MATCH clause must end with a node, not an edge.");

                            nextNodeAlias = pattern[pattern.Count - 1].Alias;
                        }
                        else
                        {
                            if (pattern[pattern.Count - 1].IsEdge == false)
                                throw new InvalidOperationException("Recursive expression that is not the last element in the MATCH clause must end with an ege, not a node.");

                            nextNodeAlias = ee.Path[pathIndex + 1].Alias;
                        }

                        EnsureValidNextAlias(nextNodeAlias);

                        ProcessRecursiveMatchPart(currentResults, aliases, matchPath.Recursive.Value, prevNodeAlias, nextNodeAlias);

                        pathIndex+=2;

                        if(!atEnd)
                        {
                            // we aren't the last item in the pattern, but 
                            // the next one is a node, so we need to process the edges 
                            // from the recursive to it

                            MatchPath lastMatch = matchPath.Recursive.Value.Pattern.Last();
                            var extrator = new RecursiveExtrator(matchPath.Recursive.Value.Alias, lastMatch.Alias, nextNodeAlias);
                            ProcessSingleMatchPart(currentResults, aliases, lastMatch, prevNodeAlias, nextNodeAlias, extrator);
                        }


                    }

                }

                _aliasesInMatch.Add(ee, aliases); //if we don't visit each match pattern exactly once, we have an issue 

                var listMatches = _intermediateOutputs[ee];
                foreach (var item in currentResults)
                {
                    if (item.Empty == false)
                        listMatches.Add(item);
                }
            }

            private void EnsureValidNextAlias(StringSegment nextNodeAlias)
            {
                if (_mapReduceAliases.Contains(nextNodeAlias))
                {
                    throw new InvalidOperationException("Target vertices in a pattern match that originate from map/reduce WITH clause are not allowed. (pattern match has multiple statements in the form of (a)-[:edge]->(b) ==> in such pattern, 'b' must not originate from map/reduce index query)");
                }
            }

            private void ProcessRecursiveMatchPart(List<Match> currentResults, HashSet<StringSegment> aliases, 
                RecursiveMatch recursive, StringSegment prevNodeAlias, StringSegment nextNodeAlias)
            {
                var currentResultsStartingSize = currentResults.Count;
                var matches = new List<Match>();
                for (int resultIndex = 0; resultIndex < currentResultsStartingSize; resultIndex++)
                {
                    matches.Clear();
                    if (TryGetMatchRecursive(currentResults[resultIndex], recursive, prevNodeAlias, nextNodeAlias, matches))
                    {
                        bool reusedSlot = false;
                        var origin = currentResults[resultIndex];
                        foreach (var match in matches)
                        {
                            var clone = new Match(origin);
                            clone.Set(recursive.Alias, match.GetResult(recursive.Alias));

                            if (reusedSlot)
                            {
                                currentResults.Add(clone);
                            }
                            else
                            {
                                reusedSlot = true;
                                currentResults[resultIndex] = clone;
                            }
                        }
                    }
                    else
                    {
                        currentResults[resultIndex] = default;
                    }
                }
            }

            private class RecursionState
            {
                public BlittableJsonReaderObject Src;
                public List<Match> Matches;
                public Match Match;
                public bool AlreadyAdded;
            }

            private bool TryGetMatchRecursive(Match currentMatch, RecursiveMatch recursive, StringSegment prevNodeAlias, StringSegment nextNodeAlias,
                List<Match> matches)
            {
                int? bestPathLength = null;
                var visited = new HashSet<long>();
                var path = new Stack<RecursionState>();

                var options = recursive.GetOptions(_query.Metadata, _queryParameters);

                visited.Clear();
                path.Clear();

                var originalMatch = currentMatch;
                var startingPoint = currentMatch.GetSingleDocumentResult(prevNodeAlias);
                if (startingPoint == null)
                    return false;

                visited.Add(startingPoint.Data.Location);
                path.Push(new RecursionState { Src = startingPoint.Data, Match = currentMatch });

                Document cur = startingPoint;
                bool hasResults = false;
                while (true)
                {
                    // the first item is always the root
                    if (path.Count -1 == options.Max)
                    {
                        if (AddMatch())
                            return true;
                        path.Pop();
                    }
                    else
                    {
                        if (SingleMatchInRecursivePattern(recursive, cur.Data, prevNodeAlias, nextNodeAlias, currentMatch, out var currentMatches) == false)
                        {
                            if (AddMatch())
                            {
                                return true;
                            }
                            path.Pop();
                        }
                        else
                        {
                            path.Peek().Matches = currentMatches;
                        }
                    }


                    while (true)
                    {
                        if (path.Count == 0)
                            return hasResults;

                        if (options.Type == RecursiveMatchType.Lazy &&
                            AddMatch())
                        {
                            return true;
                        }

                        var top = path.Peek();
                        if (top.Matches == null || top.Matches.Count == 0)
                        {
                            if (AddMatch())
                            {
                                 return true;
                            }

                            path.Pop();
                            visited.Remove(top.Src.Location);
                            continue;
                        }
                        currentMatch = top.Matches[top.Matches.Count - 1];
                        cur = currentMatch.GetSingleDocumentResult(nextNodeAlias);
                        top.Matches.RemoveAt(top.Matches.Count - 1);
                        if (visited.Add(cur.Data.Location) == false)
                        {
                            continue;
                        }
                        path.Push(new RecursionState { Src = cur.Data, Match = currentMatch });
                        break;
                    }
                }

                bool AddMatch()
                {
                    var top = path.Peek();
                    if (top.AlreadyAdded)
                        return false;

                    if (path.Count <= options.Min)
                        return false;

                    if(bestPathLength != null)
                    {
                        switch (options.Type)
                        {
                            case RecursiveMatchType.Longest:
                                if (path.Count <= bestPathLength.Value)
                                    return false;
                                matches.RemoveAt(0);
                                break;
                            case RecursiveMatchType.Shortest:
                                if (path.Count >= bestPathLength.Value)
                                    return false;
                                matches.RemoveAt(0);
                                break;
                        }
                    }

                    top.AlreadyAdded = true;

                    hasResults = true;
                    bestPathLength = path.Count;

                    var match = new Match();
                    var list = new List<Match>();
                    foreach (var item in path)
                    {
                        if (options.Type != RecursiveMatchType.Shortest)
                            item.AlreadyAdded = true;
                        var one = new Match();
                        foreach (var alias in recursive.Aliases)
                        {
                            var v = item.Match.GetResult(alias);
                            if (v == null)
                                continue;
                            one.Set(alias, v);
                        }
                        if (one.Empty)
                            continue;

                        list.Add(one);
                    }
                    list.Reverse();

                    match.Set(recursive.Alias, list);
                    match.Set(nextNodeAlias,cur);
                    matches.Add(match);

                    return options.Type == RecursiveMatchType.Lazy;
                }
            }

            private bool SingleMatchInRecursivePattern(RecursiveMatch recursive, BlittableJsonReaderObject src, StringSegment prevNodeAlias, StringSegment nextNodeAlias, Match currentMatch, out List<Match> matches)
            {
                matches = new List<Match>();
                for (int pathIndex = 0; pathIndex < recursive.Pattern.Count; pathIndex += 2)
                {
                    var matchPath = recursive.Pattern[pathIndex];
                    var edgeAlias = matchPath.Alias;
                    Debug.Assert(matchPath.IsEdge);
                    var edge = _gq.WithEdgePredicates[matchPath.Alias];
                    edge.EdgeAlias = edgeAlias;

                    var currentPrevNodeAlias = pathIndex == 0 ? prevNodeAlias : recursive.Pattern[pathIndex - 1].Alias;
                    var currentNextNodeAlias = pathIndex == recursive.Pattern.Count - 1 ? nextNodeAlias : recursive.Pattern[pathIndex + 1].Alias;

                    edge.FromAlias = currentPrevNodeAlias;

                    if (_mapReduceAliases.Contains(currentNextNodeAlias))
                    {
                        throw new InvalidOperationException("Target vertices in a pattern match that originate from map/reduce WITH clause are not allowed. (pattern match has multiple statements in the form of (a)-[:edge]->(b) ==> in such pattern, 'b' must not originate from map/reduce index query)");
                    }

                    if (!_source.TryGetByAlias(currentNextNodeAlias, out var edgeResults))
                        throw new InvalidOperationException("Could not fetch destination nod edge data. This should not happen and is likely a bug.");

                    if (TryGetMatches(edge, src, nextNodeAlias, edgeResults, currentMatch, matches) == false)
                    {
                        // not found, the entire chain is bad, then
                        return false;
                    }
                }
                return true;
            }

            private interface IExtractNextSource
            {
                BlittableJsonReaderObject Get(Match m, StringSegment alias, Dictionary<string, Match> edgeResults);
            }

            private struct DirectExtrator : IExtractNextSource
            {
                public BlittableJsonReaderObject Get(Match m, StringSegment alias, Dictionary<string, Match> edgeResults)
                {
                    return m.GetSingleDocumentResult(alias).Data;
                }
            }

            private struct RecursiveExtrator : IExtractNextSource
            {
                private readonly StringSegment _recursiveAlias;
                private readonly StringSegment _edgeAlias;
                private readonly StringSegment _nextAlias;

                public RecursiveExtrator(StringSegment recursiveAlias, StringSegment edgeAlias, StringSegment nextAlias)
                {
                    _recursiveAlias = recursiveAlias;
                    _edgeAlias = edgeAlias;
                    _nextAlias = nextAlias;
                }

                public BlittableJsonReaderObject Get(Match m, StringSegment alias, Dictionary<string, Match> edgeResults)
                {
                    var matches = (List<Match>)m.GetResult(_recursiveAlias);
                    if (matches.Count == 0)
                    {
                        var result = m.GetSingleDocumentResult(alias);
                        return result?.Data;
                    }
                    int index = matches.Count - 2;
                    if (matches.Count == 1)
                        index = 0;

                    // the last item in the list is the _next_ item, we need to go back another round
                    var key = (string)matches[index].GetResult(_edgeAlias);
                    if (edgeResults.TryGetValue(key, out var match))
                        return match.GetSingleDocumentResult(_nextAlias).Data;
                    return null;
                }
            }


            private void ProcessSingleMatchPart<TExtrator>(List<Match> currentResults, HashSet<StringSegment> aliases, MatchPath matchPath, StringSegment prevNodeAlias, StringSegment nextNodeAlias,
                TExtrator extrator)
                where TExtrator : struct, IExtractNextSource
            {
                var edgeAlias = matchPath.Alias;
                var edge = _gq.WithEdgePredicates[edgeAlias];
                edge.EdgeAlias = edgeAlias;
                edge.FromAlias = prevNodeAlias;

                aliases.Add(prevNodeAlias);
                aliases.Add(nextNodeAlias);

                if (!_source.TryGetByAlias(nextNodeAlias, out var edgeResults))
                    throw new InvalidOperationException("Could not fetch destination nod edge data. This should not happen and is likely a bug.");

                var currentResultsStartingSize = currentResults.Count;
                for (int resultIndex = 0; resultIndex < currentResultsStartingSize; resultIndex++)
                {
                    var edgeResult = currentResults[resultIndex];

                    if (edgeResult.Empty)
                        continue;

                    _results.Clear();
                    var src = extrator.Get(edgeResult, prevNodeAlias, edgeResults);
                    if (src  != null && TryGetMatches(edge, src, nextNodeAlias, edgeResults, edgeResult, _results))
                    {
                        bool reusedSlot = false;
                        foreach (var match in _results)
                        {
                            if (reusedSlot)
                            {
                                currentResults.Add(match);
                            }
                            else
                            {
                                reusedSlot = true;
                                currentResults[resultIndex] = match;
                            }

                        }
                        continue;
                    }

                    //if didn't find multiple AND single edges, then it has no place in query results...
                    currentResults[resultIndex] = default;
                }
            }

            private bool TryGetMatches(WithEdgesExpression edge, BlittableJsonReaderObject src, string alias, Dictionary<string, Match> edgeResults, Match edgeResult,
               List<Match> relatedMatches)
            {
                bool hasResults = false;
                if (edge.Where != null || edge.Project != null)
                {
                    if(BlittableJsonTraverser.Default.TryRead(src, edge.Path.FieldValue, out var value, out _) == false)
                        return false;

                    switch (value)
                    {
                        case BlittableJsonReaderArray array:
                            foreach (var item in array)
                            {
                                if (item is BlittableJsonReaderObject json &&
                                    edge.Where?.IsMatchedBy(json, _queryParameters) != false)
                                {
                                    hasResults |= TryGetMatchesAfterFiltering(edgeResult, json, edge.Project.FieldValue, edgeResults, alias, edge.EdgeAlias, relatedMatches);
                                }
                            }
                            break;
                        case BlittableJsonReaderObject json:
                            if (edge.Where?.IsMatchedBy(json, _queryParameters) != false)
                            {
                                hasResults |= TryGetMatchesAfterFiltering(edgeResult, json, edge.Project.FieldValue, edgeResults, alias, edge.EdgeAlias, relatedMatches);
                            }
                            break;
                    }
                    return hasResults;
                }
                else 
                {
                    hasResults = TryGetMatchesAfterFiltering(edgeResult, src, edge.Path.FieldValue, edgeResults, alias, edge.EdgeAlias, relatedMatches);
                }

                if (hasResults)
                    ProcessResults();

                return hasResults;

                void ProcessResults()
                {
                    for (int i = 0; i < relatedMatches.Count; i++)
                    {
                        var related = relatedMatches[i].GetSingleDocumentResult(alias);
                        var relatedEdge = relatedMatches[i].GetResult(edge.EdgeAlias);
                        var updatedMatch = new Match(edgeResult);

                        updatedMatch.Set(edge.EdgeAlias, relatedEdge);
                        updatedMatch.Set(alias, related);

                        relatedMatches[i] = updatedMatch;
                    }
                }
            }

            private struct IncludeEdgeOp : IncludeUtil.IIncludeOp
            {
                private GraphExecuteVisitor _parent;

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

            private unsafe bool TryGetMatchesAfterFiltering(
                Match previous,
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

                bool hasResults = false;
                if (edgeResults == null)
                {
                    foreach (var kvp in _includedEdges)
                    {
                        var doc = _ctx.DocumentDatabase.DocumentsStorage.Get(_ctx, kvp.Key, false);
                        if (doc == null)
                            continue;

                        var m = new Match(previous);

                        m.Set(docAlias, doc);
                        if (ShouldUseFullObjectForEdge(src, kvp.Value))
                            m.Set(edgeAlias, kvp.Value);
                        else
                            m.Set(edgeAlias, kvp.Key);


                        hasResults = true;
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

                        var clone = new Match(previous);
                        clone.Merge(m);

                        if (ShouldUseFullObjectForEdge(src, kvp.Value))
                            clone.Set(edgeAlias, kvp.Value);
                        else
                            clone.Set(edgeAlias, kvp.Key);

                        hasResults = true;
                        results.Add(clone);
                    }
                }

                return hasResults;
            }

            private static unsafe bool ShouldUseFullObjectForEdge(BlittableJsonReaderObject src,  Document json)
            {
                return json != null && (json.Data != src || src.HasParent);
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
