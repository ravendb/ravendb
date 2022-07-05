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
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                await GetDocumentsAndWriteAsync(context, RequestHandler.GetStart(), RequestHandler.GetPageSize(), RequestHandler.GetStringQueryString("startsWith", required: false),
                    RequestHandler.GetStringQueryString("excludes", required: false), RequestHandler.GetStringQueryString("matches", required: false),
                    RequestHandler.GetStringQueryString("startAfter", required: false));
            }
        }
    }
}
