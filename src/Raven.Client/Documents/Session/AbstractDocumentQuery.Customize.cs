using System;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session.Operations;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///   A query against a Raven index
    /// </summary>
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected Action<IndexQuery> BeforeQueryExecutedCallback;

        protected Action<QueryResult> AfterQueryExecutedCallback;

        protected Action<BlittableJsonReaderObject> AfterStreamExecutedCallback;

        public QueryOperation QueryOperation { get; protected set; }

        /// <inheritdoc />
        public IDocumentQueryCustomization BeforeQueryExecuted(Action<IndexQuery> action)
        {
            BeforeQueryExecutedCallback += action;
            return this;
        }

        /// <inheritdoc />
        public IDocumentQueryCustomization AfterQueryExecuted(Action<QueryResult> action)
        {
            AfterQueryExecutedCallback += action;
            return this;
        }

        /// <inheritdoc />
        public IDocumentQueryCustomization AfterStreamExecuted(Action<BlittableJsonReaderObject> action)
        {
            AfterStreamExecutedCallback += action;
            return this;
        }

        /// <inheritdoc />
        public IDocumentQueryCustomization NoTracking()
        {
            DisableEntitiesTracking = true;
            return this;
        }

        /// <inheritdoc />
        public IDocumentQueryCustomization NoCaching()
        {
            DisableCaching = true;
            return this;
        }

        /// <inheritdoc />
        public IDocumentQueryCustomization Timings(out QueryTimings timings)
        {
            IncludeTimings(out timings);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.RandomOrdering()
        {
            RandomOrdering();
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.RandomOrdering(string seed)
        {
            RandomOrdering(seed);
            return this;
        }

#if FEATURE_CUSTOM_SORTING
        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.CustomSortUsing(string typeName)
        {
            CustomSortUsing(typeName, false);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.CustomSortUsing(string typeName, bool descending)
        {
            CustomSortUsing(typeName, descending);
            return this;
        }
#endif

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults(TimeSpan? waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.Projection(ProjectionBehavior projectionBehavior)
        {
            Projection(projectionBehavior);
            return this;
        }
    }
}
