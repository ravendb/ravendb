using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries.Graph
{
    public class QueryPlanVisitor
    {
        public virtual void Visit(IGraphQueryStep root)
        {
            switch (root)
            {
                case QueryQueryStep qqs:
                    VisitQueryQueryStep(qqs);
                    return;
                case EdgeQueryStep eqs:
                    VisitEdgeQueryStep(eqs);
                    return;
                case CollectionDestinationQueryStep cdqs:
                    VisitCollectionDestinationQueryStep(cdqs);
                    return;
                case IntersectionQueryStep<Except> iqse:
                    VisitIntersectionQueryStepExcept(iqse);
                    return;
                case IntersectionQueryStep<Union> iqsu:
                    VisitIntersectionQueryStepUnion(iqsu);
                    return;
                case IntersectionQueryStep<Intersection> iqsi:
                    VisitIntersectionQueryStepIntersection(iqsi);
                    return;
                case RecursionQueryStep rqs:
                    VisitRecursionQueryStep(rqs);
                    return;
                case ForwardedQueryStep fqs:
                    VisitForwardedQueryStep(fqs);
                    return;
                default:
                    throw new NotSupportedException($"Unexpected type {root.GetType().Name} for QueryPlanVisitor.Visit");

            }
        }

        public virtual void VisitForwardedQueryStep(ForwardedQueryStep fqs)
        {
            Visit(fqs.ForwardedStep);
        }

        public virtual void VisitQueryQueryStep(QueryQueryStep qqs)
        {
        }

        public virtual void VisitEdgeQueryStep(EdgeQueryStep eqs)
        {
            Visit(eqs.Left);
            Visit(eqs.Right);
        }

        public virtual void VisitCollectionDestinationQueryStep(CollectionDestinationQueryStep cdqs)
        {
        }

        public virtual void VisitIntersectionQueryStepExcept(IntersectionQueryStep<Except> iqse)
        {
            Visit(iqse.Left);
            Visit(iqse.Right);
        }

        public virtual void VisitIntersectionQueryStepUnion(IntersectionQueryStep<Union> iqsu)
        {
            Visit(iqsu.Left);
            Visit(iqsu.Right);            
        }

        public virtual void VisitIntersectionQueryStepIntersection(IntersectionQueryStep<Intersection> iqsi)
        {
            Visit(iqsi.Left);
            Visit(iqsi.Right);
        }

        public virtual void VisitRecursionQueryStep(RecursionQueryStep rqs)
        {
            Visit(rqs.Left);

            foreach (var step in rqs.Steps)
            {
                Visit(step.Right);
            }
        }
    }
}
