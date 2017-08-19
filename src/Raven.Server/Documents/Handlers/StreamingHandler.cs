using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Transformers;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class StreamingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/streams/docs", "HEAD", AuthorizationStatus.ValidUser)]
        public Task StreamDocsHead()
        {
            //why is this action exists in 3.0?
            //TODO: review the need for this endpoint			
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/docs", "GET", AuthorizationStatus.ValidUser)]
        public Task StreamDocsGet()
        {
            var transformerName = GetStringQueryString("transformer", required: false);
            var start = GetStart();
            var pageSize = GetPageSize();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                IEnumerable<Document> documents;
                if (HttpContext.Request.Query.ContainsKey("startsWith"))
                {
                    documents = Database.DocumentsStorage.GetDocumentsStartingWith(context,
                        HttpContext.Request.Query["startsWith"],
                        HttpContext.Request.Query["matches"],
                        HttpContext.Request.Query["excludes"],
                        HttpContext.Request.Query["startAfter"],
                        start,
                        pageSize);
                }
                else // recent docs
                {
                    documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, start, pageSize);
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");

                    writer.WriteDocuments(context, documents, metadataOnly: false, numberOfResults: out int _);

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/queries", "HEAD", AuthorizationStatus.ValidUser)]
        public Task SteamQueryHead()
        {
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/queries", "POST", AuthorizationStatus.ValidUser)]
        public async Task StreamQueryGet()
        {
            using (TrackRequestTime())
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var queryJson = await context.ReadForMemoryAsync(RequestBodyStream(), "index/query");
                var query = IndexQueryServerSide.Create(queryJson, context, Database.QueryMetadataCache);

                var runner = new QueryRunner(Database, context);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    try
                    {
                        await runner.ExecuteStreamQuery(query, HttpContext.Response, writer, token).ConfigureAwait(false);
                    }
                    catch (IndexDoesNotExistException)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    }
                }
            }
        }
    }
}
