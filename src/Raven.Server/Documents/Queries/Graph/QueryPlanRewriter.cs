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
                case ForwardedQueryStep fqs:
                    return VisitForwardedQueryStep(fqs);
                default:
                    throw new NotSupportedException($"Unexpected type {root.GetType().Name} for QueryPlanRewriter.Visit");

            }
        }

        public virtual IGraphQueryStep VisitForwardedQueryStep(ForwardedQueryStep fqs)
        {
            var forwarded = Visit(fqs.ForwardedStep);
            if (ReferenceEquals(forwarded, fqs.ForwardedStep))
                return fqs;
            return new ForwardedQueryStep(forwarded, fqs.GetOutputAlias());
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

            return new IntersectionQueryStep<Except>(left, right);
        }

        public virtual IGraphQueryStep VisitIntersectionQueryStepUnion(IntersectionQueryStep<Union> iqsu)
        {
            var left = Visit(iqsu.Left);
            var right = Visit(iqsu.Right);

            return new IntersectionQueryStep<Union>(left, right, returnEmptyIfLeftEmpty:false);
        }

        public virtual IGraphQueryStep VisitIntersectionQueryStepIntersection(IntersectionQueryStep<Intersection> iqsi)
        {
            var left = Visit(iqsi.Left);
            var right = Visit(iqsi.Right);

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

            if (modified)
            {
                return new RecursionQueryStep(rqs, left, steps);
            }

            return new RecursionQueryStep(left, rqs);
        }
    }
}
