using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Exceptions.Transformers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Transformers;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class StreamingHandler : DatabaseRequestHandler
    {
        private string _postQuery;

        [RavenAction("/databases/*/streams/docs", "HEAD", "/databases/{databaseName:string}/streams/docs")]
        public Task StreamDocsHead()
        {
            //why is this action exists in 3.0?
            //TODO: review the need for this endpoint			
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/docs", "GET", "/databases/{databaseName:string}/streams/docs")]
        public Task StreamDocsGet()
        {
            var transformerName = GetStringQueryString("transformer", required: false);
            var start = GetStart();
            var pageSize = GetPageSize();

            Transformer transformer = null;
            if (string.IsNullOrEmpty(transformerName) == false)
            {
                transformer = Database.TransformerStore.GetTransformer(transformerName);
                if (transformer == null)
                    TransformerDoesNotExistException.ThrowFor(transformerName);
            }

            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var etag = GetLongQueryString("etag", false);
                IEnumerable<Document> documents;
                if (etag != null)
                {
                    documents = Database.DocumentsStorage.GetDocumentsFrom(context, etag.Value, start, pageSize);
                }
                else if (HttpContext.Request.Query.ContainsKey("startsWith"))
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

                    if (transformer != null)
                    {
                        using (var transformerParameters = GetTransformerParameters(context))
                        {
                            using (var scope = transformer.OpenTransformationScope(transformerParameters, null,
                                Database.DocumentsStorage,
                                Database.TransformerStore, context))
                            {
                                writer.WriteDocuments(context, scope.Transform(documents), metadataOnly: false, numberOfResults: out int _);
                            }
                        }
                    }
                    else
                    {
                        writer.WriteDocuments(context, documents, metadataOnly: false, numberOfResults: out int _);
                    }

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/queries/$", "HEAD")]
        public Task SteamQueryHead()
        {
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/queries/$", "GET")]
        public async Task StreamQueryGet()
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            DocumentsOperationContext context;
            using (TrackRequestTime())
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var query = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(), context);
                if (string.IsNullOrWhiteSpace(query.Query))
                    query.Query = _postQuery;

                var runner = new QueryRunner(Database, context);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    try
                    {
                        await runner.ExecuteStreamQuery(indexName, query, HttpContext.Response, writer, token).ConfigureAwait(false);
                    }
                    catch (IndexDoesNotExistException)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    }
                }
            }
        }

        [RavenAction("/databases/*/streams/queries/$", "POST")]
        public Task StreamQueryPost()
        {
            using (var sr = new StreamReader(RequestBodyStream()))
                _postQuery = sr.ReadToEnd();

            return StreamQueryGet();
        }
    }
}
