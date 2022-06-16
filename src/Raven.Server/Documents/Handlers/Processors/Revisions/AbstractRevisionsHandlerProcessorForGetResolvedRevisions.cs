using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForGetResolvedRevisions<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractRevisionsHandlerProcessorForGetResolvedRevisions([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask GetResolvedRevisionsAndWriteAsync(TOperationContext context, DateTime since, int take, CancellationToken token);
        
        public override async ValueTask ExecuteAsync()
        {
            var since = RequestHandler.GetStringQueryString("since", required: false);
            var take = RequestHandler.GetIntValueQueryString("take", required: false) ?? 1024;
            var date = Convert.ToDateTime(since).ToUniversalTime();

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var token = RequestHandler.CreateOperationToken())
            {
                await GetResolvedRevisionsAndWriteAsync(context, since: date, take, token.Token);
            }
        }
    }
}
