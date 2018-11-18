using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Documents.Queries.Graph
{
    public class EdgeCollectionDestinationRewriter: QueryPlanRewriter
    {
        private readonly DocumentsStorage _documentsStorage;
        private bool _isVisitingRight;

        public EdgeCollectionDestinationRewriter(DocumentsStorage documentsStorage)
        {
            _documentsStorage = documentsStorage;
        }

        public override IGraphQueryStep VisitEdgeQueryStep(EdgeQueryStep eqs)
        {
            var left = Visit(eqs.Left);
            _isVisitingRight = true;
            var right = Visit(eqs.Right);
            _isVisitingRight = false;
            if (ReferenceEquals(left, eqs.Left) && ReferenceEquals(right, eqs.Right))
            {
                return eqs;
            }

            return new EdgeQueryStep(left, right, eqs);
        }

        public override IGraphQueryStep VisitQueryQueryStep(QueryQueryStep qqs)
        {
            if (_isVisitingRight && qqs.IsCollectionQuery && qqs.HasWhereClause == false)
            {
                return QueryQueryStep.ToCollectionDestinationQueryStep(_documentsStorage, qqs);
            }

            return qqs;
        }

        public override IGraphQueryStep VisitRecursionQueryStep(RecursionQueryStep rqs)
        {
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
            var result = modified ? 
                new RecursionQueryStep(rqs, left, steps) : 
                new RecursionQueryStep(left, rqs);

            var next = rqs.GetNextStep();
            if(next != null)
                result.SetNext(next);

            return result;
        }
    }
}
