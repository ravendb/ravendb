using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal abstract class AbstractCollectionsHandlerProcessorForGetCollectionDocuments<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractCollectionsHandlerProcessorForGetCollectionDocuments([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask GetCollectionDocumentsAndWriteAsync(TOperationContext context, string name, int start, int pageSize, CancellationToken token);
        
        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var token = RequestHandler.CreateOperationToken())
            {
                var pageSize = RequestHandler.GetPageSize();
                var name = RequestHandler.GetStringQueryString("name");
                var start = RequestHandler.GetStart();

                await GetCollectionDocumentsAndWriteAsync(context, name, start, pageSize, token.Token);
            }
        }
    }
}
