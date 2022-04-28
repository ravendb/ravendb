using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Counters
{
    internal class CountersHandlerProcessorForPostCounters : AbstractCountersHandlerProcessorForPostCounters<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public CountersHandlerProcessorForPostCounters([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<CountersDetail> ApplyCountersOperationsAsync(DocumentsOperationContext context, CounterBatch counterBatch)
        {
            var cmd = new CountersHandler.ExecuteCounterBatchCommand(RequestHandler.Database, counterBatch);

            if (cmd.HasWrites)
            {
                try
                {
                    await RequestHandler.Database.TxMerger.Enqueue(cmd);
                }
                catch (DocumentDoesNotExistException)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    throw;
                }
            }
            else
            {
                using (context.OpenReadTransaction())
                {
                    cmd.ExecuteDirectly(context);
                }
            }

            return cmd.CountersDetail;
        }
    }
}
