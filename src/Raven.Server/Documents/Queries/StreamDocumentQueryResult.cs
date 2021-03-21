using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries
{
    public class StreamDocumentQueryResult : StreamQueryResult<Document>
    {
        public override async ValueTask AddResultAsync(Document result, CancellationToken token)
        {
            if (HasAnyWrites() == false)
                StartResponseIfNeeded();

            using (result)
                await GetWriter().AddResultAsync(result, token).ConfigureAwait(false);

            GetToken().Delay();
        }

        public StreamDocumentQueryResult(HttpResponse response, IStreamQueryResultWriter<Document> writer, OperationCancelToken token) : base(response, writer, token)
        {
            if (response.HasStarted)
                throw new InvalidOperationException("You cannot start streaming because response has already started.");
        }
    }
}
