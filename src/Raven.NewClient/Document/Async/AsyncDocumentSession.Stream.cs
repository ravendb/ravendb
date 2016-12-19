using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Linq;


namespace Raven.NewClient.Client.Document.Async
{
    public partial class AsyncDocumentSession
    {
        public Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default(CancellationToken))
        {
            return StreamAsync(query, new Reference<QueryHeaderInformation>(), token);
        }

        public Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = default(CancellationToken))
        {
            return StreamAsync(query, new Reference<QueryHeaderInformation>(), token);
        }

        public async Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(IAsyncDocumentQuery<T> query, Reference<QueryHeaderInformation> queryHeaderInformation, CancellationToken token = default(CancellationToken))
        {
            var ravenQueryInspector = ((IRavenQueryInspector)query);
            var indexQuery = ravenQueryInspector.GetIndexQuery(true);
            if (indexQuery.WaitForNonStaleResults || indexQuery.WaitForNonStaleResultsAsOfNow)
                throw new NotSupportedException(
                    "Since Stream() does not wait for indexing (by design), streaming query with WaitForNonStaleResults is not supported.");

            throw new NotImplementedException();
            /*var enumerator = await AsyncDatabaseCommands.StreamQueryAsync(ravenQueryInspector.AsyncIndexQueried, indexQuery, queryHeaderInformation, token).ConfigureAwait(false);
            var queryOperation = ((AsyncDocumentQuery<T>)query).InitializeQueryOperation();
            queryOperation.DisableEntitiesTracking = true;
            return new QueryYieldStream<T>(this, enumerator, queryOperation, query, token);*/
        }

        public async Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(IQueryable<T> query, Reference<QueryHeaderInformation> queryHeaderInformation, CancellationToken token = default(CancellationToken))
        {
            var queryInspector = (IRavenQueryProvider)query.Provider;
            var indexQuery = queryInspector.ToAsyncDocumentQuery<T>(query.Expression);
            return await StreamAsync(indexQuery, queryHeaderInformation, token).ConfigureAwait(false);
        }

        public Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(long? fromEtag, int start = 0,
                                                                    int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            return StreamAsync<T>(fromEtag: fromEtag, startsWith: null, matches: null, start: start, pageSize: pageSize, pagingInformation: pagingInformation, transformer: transformer, transformerParameters: transformerParameters, token: token);
        }

        public Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(string startsWith, string matches = null, int start = 0,
                                   int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            return StreamAsync<T>(fromEtag: null, startsWith: startsWith, matches: matches, start: start, pageSize: pageSize, pagingInformation: pagingInformation, skipAfter: skipAfter, transformer: transformer, transformerParameters: transformerParameters, token: token);
        }

        private async Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(long? fromEtag, string startsWith, string matches, int start, int pageSize, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            throw new NotImplementedException();
            /*var enumerator = await AsyncDatabaseCommands.StreamDocsAsync(fromEtag, startsWith, matches, start, pageSize, pagingInformation: pagingInformation, skipAfter: skipAfter, transformer: transformer, transformerParameters: transformerParameters, token: token).ConfigureAwait(false);
            return new DocsYieldStream<T>(this, enumerator, token);*/
        }
    }
}