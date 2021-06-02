using System;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Queries.Graph
{
    public class QueryPlanVisitor
    {
        public virtual async Task VisitAsync(IGraphQueryStep root)
        {
            switch (root)
            {
                case QueryQueryStep qqs:
                    VisitQueryQueryStep(qqs);
                    return;

                case EdgeQueryStep eqs:
                    await VisitEdgeQueryStepAsync(eqs);
                    return;

                case CollectionDestinationQueryStep cdqs:
                    VisitCollectionDestinationQueryStep(cdqs);
                    return;

                case IntersectionQueryStep<Except> iqse:
                    await VisitIntersectionQueryStepExceptAsync(iqse);
                    return;

                case IntersectionQueryStep<Union> iqsu:
                    await VisitIntersectionQueryStepUnionAsync(iqsu);
                    return;

                case IntersectionQueryStep<Intersection> iqsi:
                    await VisitIntersectionQueryStepIntersectionAsync(iqsi);
                    return;

                case RecursionQueryStep rqs:
                    await VisitRecursionQueryStepAsync(rqs);
                    return;

                default:
                    throw new NotSupportedException($"Unexpected type {root.GetType().Name} for QueryPlanVisitor.Visit");
            }
        }

        public virtual async Task VisitAsync(ISingleGraphStep step)
        {
            switch (step)
            {
                case EdgeQueryStep.EdgeMatcher em:
                    await VisitEdgeMatcherAsync(em);
                    break;

                case null:
                case QueryQueryStep.QuerySingleStep _:
                case RecursionQueryStep.RecursionSingleStep _:
                    break;
            }
        }

        public virtual async Task VisitEdgeMatcherAsync(EdgeQueryStep.EdgeMatcher em)
        {
            await VisitAsync(em._parent.Right);
        }

        public virtual void VisitQueryQueryStep(QueryQueryStep qqs)
        {
        }

        public virtual async Task VisitEdgeQueryStepAsync(EdgeQueryStep eqs)
        {
            await VisitAsync(eqs.Left);
            await VisitAsync(eqs.Right);
        }

        public virtual void VisitCollectionDestinationQueryStep(CollectionDestinationQueryStep cdqs)
        {
        }

        public virtual async Task VisitIntersectionQueryStepExceptAsync(IntersectionQueryStep<Except> iqse)
        {
            await VisitAsync(iqse.Left);
            await VisitAsync(iqse.Right);
        }

        public virtual async Task VisitIntersectionQueryStepUnionAsync(IntersectionQueryStep<Union> iqsu)
        {
            await VisitAsync(iqsu.Left);
            await VisitAsync(iqsu.Right);
        }

        public virtual async Task VisitIntersectionQueryStepIntersectionAsync(IntersectionQueryStep<Intersection> iqsi)
        {
            await VisitAsync(iqsi.Left);
            await VisitAsync(iqsi.Right);
        }

        public virtual async Task VisitRecursionQueryStepAsync(RecursionQueryStep rqs)
        {
            await VisitAsync(rqs.Left);

            foreach (var step in rqs.Steps)
            {
                await VisitAsync(step.Right);
            }

            await VisitAsync(rqs.GetNextStep());
        }
    }
}
