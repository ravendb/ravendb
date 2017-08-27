using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session.Operations.Lazy;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        /// <inheritdoc />
        public async Task<FacetedQueryResult> GetFacetsAsync(string facetSetupDoc, int facetStart, int? facetPageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(q, facetSetupDoc, null, facetStart, facetPageSize, Conventions);

            var command = new GetFacetsCommand(Conventions, TheSession.Context, query);
            await TheSession.RequestExecutor.ExecuteAsync(command, TheSession.Context, token).ConfigureAwait(false);

            return command.Result;
        }

        /// <inheritdoc />
        public async Task<FacetedQueryResult> GetFacetsAsync(List<Facet> facets, int facetStart, int? facetPageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(q, null, facets, facetStart, facetPageSize, Conventions);

            var command = new GetFacetsCommand(Conventions, TheSession.Context, query);
            await TheSession.RequestExecutor.ExecuteAsync(command, TheSession.Context, token).ConfigureAwait(false);

            return command.Result;
        }

        /// <inheritdoc />
        public Lazy<Task<FacetedQueryResult>> GetFacetsLazyAsync(string facetSetupDoc, int facetStart, int? facetPageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(q, facetSetupDoc, null, facetStart, facetPageSize, Conventions);

            var lazyFacetsOperation = new LazyFacetsOperation(Conventions, query);
            return ((AsyncDocumentSession)TheSession).AddLazyOperation<FacetedQueryResult>(lazyFacetsOperation, null, token);
        }

        /// <inheritdoc />
        public Lazy<Task<FacetedQueryResult>> GetFacetsLazyAsync(List<Facet> facets, int facetStart, int? facetPageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(q, null, facets, facetStart, facetPageSize, Conventions);

            var lazyFacetsOperation = new LazyFacetsOperation(Conventions, query);
            return ((AsyncDocumentSession)TheSession).AddLazyOperation<FacetedQueryResult>(lazyFacetsOperation, null, token);
        }
    }
}
