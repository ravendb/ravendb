using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Counters
{
    internal class CountersHandlerProcessorForGetCounters : AbstractCountersHandlerProcessorForGetCounters<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public CountersHandlerProcessorForGetCounters([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<CountersDetail> GetCountersAsync(string docId, StringValues counters, bool full)
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    return GetInternal(RequestHandler.Database, context, counters, docId, full);
                }
            }
        }

        public static CountersDetail GetInternal(DocumentDatabase database, DocumentsOperationContext context, StringValues counters, string docId, bool full)
        {
            var result = new CountersDetail();
            var names = counters.Count != 0
                ? counters
                : database.DocumentsStorage.CountersStorage.GetCountersForDocument(context, docId);

            foreach (var counter in names)
            {
                GetCounterValue(context, database, docId, counter, full, result);
            }

            return result;
        }
    }
}
