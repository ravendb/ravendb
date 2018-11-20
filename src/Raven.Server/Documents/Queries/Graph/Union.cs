using System.Collections.Generic;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public struct Union : ISetOp
    {
        public bool CanOptimizeSides => true;
        public bool ShouldContinueWhenNoIntersection => true;
        private HashSet<string> _leftAliases, _rightAliases;


        public void Set(IGraphQueryStep left, IGraphQueryStep right)
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
                var val = src.GetResult(alias);
                if (val == null)
                    continue;
                dst.Set(alias, val);
            }
        }

    }
}
