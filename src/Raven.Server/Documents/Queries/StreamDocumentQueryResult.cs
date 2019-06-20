using System;
using Microsoft.AspNetCore.Http;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries
{
    public class StreamDocumentQueryResult : StreamQueryResult<Document>
    {
        public override void AddResult(Document result)
        {
            if (HasAnyWrites() == false)
                StartResponseIfNeeded();

            using (result.Data)
                GetWriter().AddResult(result);
            GetToken().Delay();
        }

        public StreamDocumentQueryResult(HttpResponse response, IStreamQueryResultWriter<Document> writer, OperationCancelToken token) : base(response, writer, token)
        {
            if (response.HasStarted)
                throw new InvalidOperationException("You cannot start streaming because response has already started.");
        }
    }
}
