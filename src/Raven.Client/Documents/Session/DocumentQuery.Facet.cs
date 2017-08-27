using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session.Operations.Lazy;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentQuery<T>
    {
        /// <inheritdoc />
        public FacetedQueryResult GetFacets(string facetSetupDoc, int facetStart, int? facetPageSize)
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(q, facetSetupDoc, null, facetStart, facetPageSize, Conventions);

            var command = new GetFacetsCommand(Conventions, TheSession.Context, query);
            TheSession.RequestExecutor.Execute(command, TheSession.Context);

            return command.Result;
        }

        /// <inheritdoc />
        public FacetedQueryResult GetFacets(List<Facet> facets, int facetStart, int? facetPageSize)
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(q, null, facets, facetStart, facetPageSize, Conventions);

            var command = new GetFacetsCommand(Conventions, TheSession.Context, query);
            TheSession.RequestExecutor.Execute(command, TheSession.Context);

            return command.Result;
        }

        /// <inheritdoc />
        public Lazy<FacetedQueryResult> GetFacetsLazy(string facetSetupDoc, int facetStart, int? facetPageSize)
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(q, facetSetupDoc, null, facetStart, facetPageSize, Conventions);

            var lazyFacetsOperation = new LazyFacetsOperation(Conventions, query);
            return ((DocumentSession)TheSession).AddLazyOperation(lazyFacetsOperation, (Action<FacetedQueryResult>)null);
        }

        /// <inheritdoc />
        public Lazy<FacetedQueryResult> GetFacetsLazy(List<Facet> facets, int facetStart, int? facetPageSize)
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(q, null, facets, facetStart, facetPageSize, Conventions);

            var lazyFacetsOperation = new LazyFacetsOperation(Conventions, query);
            return ((DocumentSession)TheSession).AddLazyOperation(lazyFacetsOperation, (Action<FacetedQueryResult>)null);
        }
    }
}
