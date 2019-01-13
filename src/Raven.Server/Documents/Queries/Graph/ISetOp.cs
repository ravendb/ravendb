using System.Collections.Generic;
using Raven.Server.ServerWide;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public interface ISetOp
    {
        void Op(List<Match> output,
            Match left,
            Match right,
            bool allIntersectionsMatch,
            HashSet<Match> state);

        void Set(IGraphQueryStep left, IGraphQueryStep right);

        bool CanOptimizeSides { get; }
        bool ShouldContinueWhenNoIntersection { get; }
        void Complete(List<Match> output, Dictionary<long, List<Match>> intersection, HashSet<Match> state, OperationCancelToken token);
    }
}
