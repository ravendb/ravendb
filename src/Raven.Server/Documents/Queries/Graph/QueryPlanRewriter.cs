using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Documents.Queries.Graph
{
    public class QueryPlanRewriter
    {
        public virtual IGraphQueryStep Visit(IGraphQueryStep root)
        {
            switch (root)
            {
                case QueryQueryStep qqs:
                    return VisitQueryQueryStep(qqs);
                case EdgeQueryStep eqs:
                    return VisitEdgeQueryStep(eqs);
                case CollectionDestinationQueryStep cdqs:
                    return VisitCollectionDestinationQueryStep(cdqs);
                case IntersectionQueryStep<Except> iqse:
                    return VisitIntersectionQueryStepExcept(iqse);
                case IntersectionQueryStep<Union> iqsu:
                    return VisitIntersectionQueryStepUnion(iqsu);
                case IntersectionQueryStep<Intersection> iqsi:
                    return VisitIntersectionQueryStepIntersection(iqsi);
                case RecursionQueryStep rqs:
                    return VisitRecursionQueryStep(rqs);
                default:
                    throw new NotSupportedException($"Unexpected type {root.GetType().Name} for QueryPlanRewriter.Visit");

            }
        }

        public virtual void Visit(ISingleGraphStep step)
        {
            switch (step)
            {
                case EdgeQueryStep.EdgeMatcher em:
                     VisitEdgeMatcher(em);
                    break;
                case null:
                case QueryQueryStep.QuerySingleStep _:
                case RecursionQueryStep.RecursionSingleStep _:
                    break;
            }
        }

        //This method is only invoked from recursive step and fix the problem that we don't visit edges after recursive steps
        public virtual void VisitEdgeMatcher(EdgeQueryStep.EdgeMatcher em)
        {
            var newRight = Visit(em._parent.Right);
            if (ReferenceEquals(newRight, em._parent.Right) == false)
            {
                em._parent.SetRight(newRight);
            }
        }

        public virtual IGraphQueryStep VisitQueryQueryStep(QueryQueryStep qqs)
        {
            return qqs;
        }

        public virtual IGraphQueryStep VisitEdgeQueryStep(EdgeQueryStep eqs)
        {
            var left = Visit(eqs.Left);
            var right = Visit(eqs.Right);

            if (ReferenceEquals(left, eqs.Left) && ReferenceEquals(right, eqs.Right))
            {
                return eqs;
            }

            return new EdgeQueryStep(left, right, eqs);
        }

        public virtual IGraphQueryStep VisitCollectionDestinationQueryStep(CollectionDestinationQueryStep cdqs)
        {
            return cdqs;
        }

        public virtual IGraphQueryStep VisitIntersectionQueryStepExcept(IntersectionQueryStep<Except> iqse)
        {
            var left = Visit(iqse.Left);
            var right = Visit(iqse.Right);

            if (ReferenceEquals(left, iqse.Left) && ReferenceEquals(right, iqse.Right))
                return iqse;

            return new IntersectionQueryStep<Except>(left, right);
        }

        public virtual IGraphQueryStep VisitIntersectionQueryStepUnion(IntersectionQueryStep<Union> iqsu)
        {
            var left = Visit(iqsu.Left);
            var right = Visit(iqsu.Right);

            if (ReferenceEquals(left, iqsu.Left) && ReferenceEquals(right, iqsu.Right))
                return iqsu;

            return new IntersectionQueryStep<Union>(left, right, returnEmptyIfLeftEmpty:false);
        }

        public virtual IGraphQueryStep VisitIntersectionQueryStepIntersection(IntersectionQueryStep<Intersection> iqsi)
        {
            var left = Visit(iqsi.Left);
            var right = Visit(iqsi.Right);

            if (ReferenceEquals(left, iqsi.Left) && ReferenceEquals(right, iqsi.Right))
                return iqsi;

            return new IntersectionQueryStep<Intersection>(left, right, returnEmptyIfRightEmpty: true);
        }

        public virtual IGraphQueryStep VisitRecursionQueryStep(RecursionQueryStep rqs)
        {
            var left = Visit(rqs.Left);
            bool modified = ReferenceEquals(left, rqs.Left) == false;

            var steps = new List<SingleEdgeMatcher>();

            foreach (var step in rqs.Steps)
            {
                var right = Visit(step.Right);
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
            if (next != null)
            {
                Visit(next);
            }

            if (modified)
            {
                return new RecursionQueryStep(rqs, left, steps);
            }

            return rqs;
        }
    }
}
