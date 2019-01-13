using System.Collections.Generic;
using Raven.Server.ServerWide;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public struct Except : ISetOp
    {
        // for AND NOT, the sides really matter, so we can't optimize it
        public bool CanOptimizeSides => false;
        public bool ShouldContinueWhenNoIntersection => true;
        private HashSet<string> _leftAliases, _rightAliases;

        public void Set(IGraphQueryStep left, IGraphQueryStep right)
        {
            _leftAliases = left.GetAllAliases();
            _rightAliases = right.GetAllAliases();
        }

        public void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<Match> state, OperationCancelToken token)
        {
            foreach (var kvp in intersection)
            {
                foreach (var item in kvp.Value)
                {
                    token.CheckIfCancellationIsRequested();
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
