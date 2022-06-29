using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Streaming
{
    internal abstract class AbstractStreamingHandlerProcessorForGetDocs<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractStreamingHandlerProcessorForGetDocs([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask GetDocumentsAndWriteAsync(TOperationContext context, int start, int pageSize, string startsWith,
            string excludes, string matches, string startAfter);

        public override async ValueTask ExecuteAsync()
        {
            var start = RequestHandler.GetStart();
            var pageSize = RequestHandler.GetPageSize();

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                await GetDocumentsAndWriteAsync(context, start, pageSize, HttpContext.Request.Query["startsWith"],
                    HttpContext.Request.Query["excludes"], HttpContext.Request.Query["matches"], HttpContext.Request.Query["startAfter"]);
            }
        }
    }
}
