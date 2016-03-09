using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

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
    }
}
