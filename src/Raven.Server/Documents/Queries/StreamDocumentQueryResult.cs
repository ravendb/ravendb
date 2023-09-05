using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries
{
    public sealed class StreamDocumentQueryResult : StreamQueryResult<Document>
    {
        private readonly DocumentsOperationContext _context;

        public override async ValueTask AddResultAsync(Document result, CancellationToken token)
        {
            if (HasAnyWrites() == false)
                StartResponseIfNeeded();

            using (result)
                await GetWriter().AddResultAsync(result, token).ConfigureAwait(false);

            _context.Transaction.InnerTransaction.ForgetAbout(result.StorageId);

            GetToken().Delay();
        }

        public StreamDocumentQueryResult(HttpResponse response, IStreamQueryResultWriter<Document> writer, DocumentsOperationContext context,
            long? indexDefinitionRaftIndex, OperationCancelToken token) : base(response, writer, indexDefinitionRaftIndex, token)
        {
            if (response.HasStarted)
                throw new InvalidOperationException("You cannot start streaming because response has already started.");
            _context = context;
        }
    }
}
