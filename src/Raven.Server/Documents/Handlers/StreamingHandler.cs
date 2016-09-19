using System;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class StreamingHandler : DatabaseRequestHandler
    {
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
        public Task StreamQueryGet()
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            DocumentsOperationContext context;
            using (TrackRequestTime())
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var query = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(int.MaxValue), context);

                var runner = new QueryRunner(Database, context);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    runner.ExecuteStreamQuery(indexName, query, HttpContext.Response, writer, token);
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/queries/$", "POST")]
        public Task StreamQueryPost()
        {
            throw new NotImplementedException();
        }
    }
}
