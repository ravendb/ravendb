using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Queries.AST;
using Sparrow;

namespace Raven.Server.Documents.Queries
{
    public partial class GraphQueryRunner
    {
        private partial class GraphExecuteVisitor
        {
            private interface ISetOp
            {
                void Op(List<Match> output,
                    (Match Match, HashSet<StringSegment> Aliases) left,
                    (Match Match, HashSet<StringSegment> Aliases) right,
                    bool allIntersectionsMatch,
                    HashSet<Match> state);

                bool CanOptimizeSides { get; }
                bool ShouldContinueWhenNoIntersection { get; }
                void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<StringSegment> aliases, HashSet<Match> state);
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
                    foreach (var kvp in intersection)
                    {
                        foreach (var item in kvp.Value)
                        {
                            if (state.Contains(item) == false)
                            {
                                output.Add(item);
                            }
                        }
                    }

                    foreach (var nonIntersectedItem in state)
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
                if (xOutput.Count < yOutput.Count && operation.CanOptimizeSides)
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

        }
    }
}
