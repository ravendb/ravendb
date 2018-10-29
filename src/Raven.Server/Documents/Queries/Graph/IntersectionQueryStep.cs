using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class IntersectionQueryStep<TOp> : IQueryStep
        where TOp : struct, ISetOp
    {
        private Dictionary<long, List<Match>> _tempIntersect = new Dictionary<long, List<Match>>();
        private HashSet<string> _unionedAliases;
        private List<string> _intersectedAliases;
        private readonly IQueryStep _left;
        private readonly IQueryStep _right;
        private readonly List<Match> _results = new List<Match>();
        private int _index;

        public IntersectionQueryStep(IQueryStep left, IQueryStep right)
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

        public async ValueTask Initialize()
        {
            //TODO:
            await _left.Initialize();
            await _right.Initialize();

            IntersectExpressions();
        }

        public HashSet<string> GetAllAliases()
        {
            return _unionedAliases;
        }

        public string GetOuputAlias()
        {
            return _right.GetOuputAlias();
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
    }

    public interface ISetOp
    {
        void Op(List<Match> output,
            Match left,
            Match right,
            bool allIntersectionsMatch,
            HashSet<Match> state);

        void Set(IQueryStep left, IQueryStep right);

        bool CanOptimizeSides { get; }
        bool ShouldContinueWhenNoIntersection { get; }
        void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<Match> state);
    }

    public struct Intersection : ISetOp
    {
        public bool CanOptimizeSides => true;
        public bool ShouldContinueWhenNoIntersection => false;

        private HashSet<string> _leftAliases, _rightAliases;

        public void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<StringSegment> aliases, HashSet<Match> state)
        {
            // nothing to do
        }

        public void Op(List<Match> output,
            Match left,
            Match right,
            bool allIntersectionsMatch,
            HashSet<Match> state)
        {
            if (allIntersectionsMatch == false)
                return;

            var resultMatch = new Match();

            Union.CopyAliases(left, ref resultMatch, _leftAliases);
            Union.CopyAliases(right, ref resultMatch, _rightAliases);
            output.Add(resultMatch);
        }

        public void Set(IQueryStep left, IQueryStep right)
        {
            _leftAliases = left.GetAllAliases();
            _rightAliases = right.GetAllAliases();
        }

        public void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<Match> state)
        {
            
        }
    }

    public struct Union : ISetOp
    {
        public bool CanOptimizeSides => true;
        public bool ShouldContinueWhenNoIntersection => true;
        private HashSet<string> _leftAliases, _rightAliases;


        public void Set(IQueryStep left, IQueryStep right)
        {
            _leftAliases = left.GetAllAliases();
            _rightAliases = right.GetAllAliases();
        }

        public void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<Match> state)
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
            Match left,
            Match right,
            bool allIntersectionsMatch,
            HashSet<Match> state)
        {
            if (allIntersectionsMatch == false)
            {
                output.Add(right);
                return;
            }

            var resultMatch = new Match();

            CopyAliases(left, ref resultMatch, _leftAliases);
            CopyAliases(right, ref resultMatch, _rightAliases);
            output.Add(resultMatch);
            state.Add(left);
        }

        public static void CopyAliases(Match src, ref Match dst, HashSet<string> aliases)
        {
            foreach (var alias in aliases)
            {
                var doc = src.GetSingleDocumentResult(alias);
                if (doc == null)
                    continue;
                dst.TrySet(alias, doc);
            }
        }

    }

    public struct Except : ISetOp
    {
        // for AND NOT, the sides really matter, so we can't optimize it
        public bool CanOptimizeSides => false;
        public bool ShouldContinueWhenNoIntersection => true;
        private HashSet<string> _leftAliases, _rightAliases;

        public void Set(IQueryStep left, IQueryStep right)
        {
            _leftAliases = left.GetAllAliases();
            _rightAliases = right.GetAllAliases();
        }

        public void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<Match> state)
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
            Match left,
            Match right,
            bool allIntersectionsMatch,
            HashSet<Match> state)
        {
            if (allIntersectionsMatch)
                state.Add(left);
        }
    }
}
