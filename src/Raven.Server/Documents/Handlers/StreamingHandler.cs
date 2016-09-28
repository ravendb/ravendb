using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Server.Documents.Queries;
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
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                HttpContext.Response.StatusCode = 200;
                HttpContext.Response.ContentType = "application/json; charset=utf-8";


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
                var query = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(int.MaxValue), context);
                if (string.IsNullOrWhiteSpace(query.Query))
                    query.Query = _postQuery;

                var runner = new QueryRunner(Database, context);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    try
                    {
                        await runner.ExecuteStreamQuery(indexName, query, HttpContext.Response, writer, token).ConfigureAwait(false);
                    }
                    catch (IndexDoesNotExistsException)
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
