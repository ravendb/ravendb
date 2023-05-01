using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
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
            string excludes, string matches, string startAfter, string format, OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
                using (var token = RequestHandler.CreateOperationToken())
                {
                    var start = RequestHandler.GetStart();
                    var pageSize = RequestHandler.GetPageSize();
                    var startsWith = RequestHandler.GetStringQueryString("startsWith", required: false);
                    var excludes = RequestHandler.GetStringQueryString("excludes", required: false);
                    var matches = RequestHandler.GetStringQueryString("matches", required: false);
                    var after = RequestHandler.GetStringQueryString("startAfter", required: false);
                    var format = RequestHandler.GetStringQueryString("format", required: false);

                    await GetDocumentsAndWriteAsync(context, start, pageSize, startsWith,
                        excludes, matches, after, format, token);
                }
        }
    }
}
