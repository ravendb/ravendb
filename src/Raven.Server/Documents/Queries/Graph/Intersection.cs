using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using Raven.Server.ServerWide;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
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

        public void Set(IGraphQueryStep left, IGraphQueryStep right)
        {
            _leftAliases = left.GetAllAliases();
            _rightAliases = right.GetAllAliases();
        }

        public void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<Match> state, OperationCancelToken token)
        {
            
        }
    }
}
