using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Linq;


namespace Raven.NewClient.Client.Document.Async
{
    public partial class AsyncDocumentSession
    {
        public async Task<IAsyncEnumerator<object>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default(CancellationToken))
        {
            throw new NotImplementedException();
            /*var StreamOperation = new StreamOperation(this);
            IEnumerator result = null;
            var command = StreamOperation.CreateRequest((IDocumentQuery<T>)query);
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context);
                result = StreamOperation.SetResult<T>(command.Result.Results);
            }

            return new AsyncEnumeratorBridge<object>(result, this);*/
        }

        public async Task<IAsyncEnumerator<object>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = default(CancellationToken))
        {
            var queryInspector = (IRavenQueryProvider)query.Provider;
            var indexQuery = queryInspector.ToAsyncDocumentQuery<T>(query.Expression);
            return await StreamAsync(indexQuery, token).ConfigureAwait(false);
        }

        public Task<IAsyncEnumerator<object>> StreamAsync<T>(long? fromEtag, int start = 0,
                                                                    int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            return StreamAsync<T>(fromEtag: fromEtag, startsWith: null, matches: null, start: start, pageSize: pageSize, pagingInformation: pagingInformation, transformer: transformer, transformerParameters: transformerParameters, token: token);
        }

        public Task<IAsyncEnumerator<object>> StreamAsync<T>(string startsWith, string matches = null, int start = 0,
                                   int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            return StreamAsync<T>(fromEtag: null, startsWith: startsWith, matches: matches, start: start, pageSize: pageSize, pagingInformation: pagingInformation, skipAfter: skipAfter, transformer: transformer, transformerParameters: transformerParameters, token: token);
        }

        private async Task<IAsyncEnumerator<object>> StreamAsync<T>(long? fromEtag, string startsWith, string matches, int start, int pageSize, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            throw new NotImplementedException();
            /*var enumerator = await AsyncDatabaseCommands.StreamDocsAsync(fromEtag, startsWith, matches, start, pageSize, pagingInformation: pagingInformation, skipAfter: skipAfter, transformer: transformer, transformerParameters: transformerParameters, token: token).ConfigureAwait(false);
            return new DocsYieldStream<T>(this, enumerator, token);*/
        }
    }
}