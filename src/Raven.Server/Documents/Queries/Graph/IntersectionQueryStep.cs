using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class IntersectionQueryStep<TOp> : IGraphQueryStep
        where TOp : struct, ISetOp
    {
        private Dictionary<long, List<Match>> _tempIntersect = new Dictionary<long, List<Match>>();
        private HashSet<string> _unionedAliases;
        private List<string> _intersectedAliases;
        private IGraphQueryStep _left;
        private readonly IGraphQueryStep _right;
        private readonly List<Match> _results = new List<Match>();
        private int _index = -1;

        public IntersectionQueryStep(IGraphQueryStep left, IGraphQueryStep right)
        {
            _unionedAliases = new HashSet<string>();
            _unionedAliases.UnionWith(left.GetAllAliases());
            _unionedAliases.UnionWith(right.GetAllAliases());

            var tmpIntersection = new HashSet<string>(left.GetAllAliases());
            tmpIntersection.IntersectWith(right.GetAllAliases());

            _intersectedAliases = tmpIntersection.ToList();

            _left = left;
            _right = right;
        }

        private void IntersectExpressions()
        {
            _index = 0;
            _tempIntersect.Clear();

            var operation = new TOp();
            operation.Set(_left, _right);
            var operationState = new HashSet<Match>();

            if (_intersectedAliases.Count == 0 && !operation.ShouldContinueWhenNoIntersection)
                return; // no matching aliases, so we need to stop when the operation is intersection

            while (_left.GetNext(out var leftMatch))
            {
                long key = GetMatchHashKey(_intersectedAliases, leftMatch);
                if (_tempIntersect.TryGetValue(key, out var matches) == false)
                    _tempIntersect[key] = matches = new List<Match>(); // TODO: pool these
                matches.Add(leftMatch);
            }


            while (_right.GetNext(out var rightMatch))
            {
                long key = GetMatchHashKey(_intersectedAliases, rightMatch);

                if (_tempIntersect.TryGetValue(key, out var matchesFromLeft) == false)
                {
                    if (operation.ShouldContinueWhenNoIntersection)
                        operationState.Add(rightMatch);
                    continue; // nothing matched, can skip
                }

                for (int i = 0; i < matchesFromLeft.Count; i++)
                {
                    var leftMatch = matchesFromLeft[i];
                    var allIntersectionsMatch = true;
                    for (int j = 0; j < _intersectedAliases.Count; j++)
                    {
                        var intersect = _intersectedAliases[j];
                        if (!leftMatch.TryGetAliasId(intersect, out var x) ||
                            !rightMatch.TryGetAliasId(intersect, out var y) ||
                            x != y)
                        {
                            allIntersectionsMatch = false;
                            break;
                        }
                    }

                    operation.Op(_results, leftMatch, rightMatch, allIntersectionsMatch, operationState);
                }
            }

            operation.Complete(_results, _tempIntersect, operationState);
        }



        private static long GetMatchHashKey(List<string> intersectedAliases, Match match)
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

        public ValueTask Initialize()
        {
            if (_index != -1)
                return default;

            var leftTask = _left.Initialize();
            if (leftTask.IsCompleted == false)
            {
                return new ValueTask(CompleteLeftInitializationAsync(leftTask));
            }

            return CompleteInitializationAfterLeft();
        }

        private ValueTask CompleteInitializationAfterLeft()
        {
            var rightTask = _right.Initialize();
            if (rightTask.IsCompleted == false)
            {
                return new ValueTask(CompleteRightInitializationAsync(rightTask));
            }
            IntersectExpressions();
            return default;
        }

        private async Task CompleteRightInitializationAsync(ValueTask rightTask)
        {
            await rightTask;
            IntersectExpressions();
        }

        private async Task  CompleteLeftInitializationAsync(ValueTask leftTask)
        {
            await leftTask;
            await CompleteInitializationAfterLeft();
        }

        public HashSet<string> GetAllAliases()
        {
            return _unionedAliases;
        }

        public string GetOutputAlias()
        {
            return _right.GetOutputAlias();
        }

        public bool GetNext(out Match match)
        {
            if(_index >= _results.Count)
            {
                match = default;
                return false;
            }
            match = _results[_index++];
            return true;
        }

        public bool TryGetById(string id, out Match match)
        {
            throw new System.NotSupportedException("Cannot pull results by id from an intersection operation");
        }

        public void Analyze(Match match, Action<string, object> addNode, Action<object, string> addEdge)
        {
            _left.Analyze(match, addNode, addEdge);
            _right.Analyze(match, addNode, addEdge);
        }
    }
}
