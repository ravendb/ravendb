using System.Collections.Generic;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries.Graph
{
    public class EdgeCollectionDestinationRewriter: QueryPlanRewriter
    {
        private readonly DocumentsStorage _documentsStorage;
        private bool _isVisitingRight;

        public EdgeCollectionDestinationRewriter(DocumentsStorage documentsStorage, OperationCancelToken token) : base(token)
        {
            _documentsStorage = documentsStorage;
        }

        public override IGraphQueryStep VisitEdgeQueryStep(EdgeQueryStep eqs)
        {
            _token.ThrowIfCancellationRequested();
            var left = Visit(eqs.Left);
            _isVisitingRight = true;
            var right = Visit(eqs.Right);
            _isVisitingRight = false;
            if (ReferenceEquals(left, eqs.Left) && ReferenceEquals(right, eqs.Right))
            {
                return eqs;
            }

            return new EdgeQueryStep(left, right, eqs, _token);
        }

        public override IGraphQueryStep VisitQueryQueryStep(QueryQueryStep qqs)
        {
            _token.ThrowIfCancellationRequested();
            if (_isVisitingRight && qqs.CanBeConsideredForDestinationOptimization)
            {
                return QueryQueryStep.ToCollectionDestinationQueryStep(_documentsStorage, qqs, _token);
            }

            return qqs;
        }

        public override void VisitEdgeMatcher(EdgeQueryStep.EdgeMatcher em)
        {
            _token.ThrowIfCancellationRequested();
            _isVisitingRight = true;
            base.VisitEdgeMatcher(em);
            _isVisitingRight = false;
        }

        public override IGraphQueryStep VisitRecursionQueryStep(RecursionQueryStep rqs)
        {
            _token.ThrowIfCancellationRequested();
            var left = Visit(rqs.Left);
            bool modified = ReferenceEquals(left, rqs.Left) == false;

            var steps = new List<SingleEdgeMatcher>();

            foreach (var step in rqs.Steps)
            {
                _isVisitingRight = true;
                var right = Visit(step.Right);
                _isVisitingRight = false;
                if (ReferenceEquals(right, step.Right) == false)
                {
                    modified = true;
                    steps.Add(new SingleEdgeMatcher(step, right));
                }
                else
                {
                    steps.Add(step);
                }
            }
            var next = rqs.GetNextStep();
            Visit(next);

            if (modified == false)
            {
                return rqs;
            }

            var result = new RecursionQueryStep(rqs, left, steps, _token);
            
            if (next != null)
            {
                next.SetPrev(result);
                result.SetNext(next);
                result.SetAliases(rqs.GetAllAliases());
            }

            return result;
        }
    }
}
