using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Document.Async
{
    public partial class AsyncDocumentSession
    {
        public Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(IAsyncDocumentQuery<T> query,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(IQueryable<T> query,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(IAsyncDocumentQuery<T> query,
            Reference<QueryHeaderInformation> queryHeaderInformation,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(IQueryable<T> query,
            Reference<QueryHeaderInformation> queryHeaderInformation,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(long? fromEtag, int start = 0,
            int pageSize = Int32.MaxValue,
            RavenPagingInformation pagingInformation = null, string transformer = null,
            Dictionary<string, object> transformerParameters = null,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<StreamResult>> StreamAsync<T>(string startsWith, string matches = null,
            int start = 0, int pageSize = Int32.MaxValue,
            RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null,
            Dictionary<string, object> transformerParameters = null,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }
    }
}