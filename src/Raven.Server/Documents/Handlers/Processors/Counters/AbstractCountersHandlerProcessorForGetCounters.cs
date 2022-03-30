using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Counters
{
    internal abstract class AbstractCountersHandlerProcessorForGetCounters<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractCountersHandlerProcessorForGetCounters([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract ValueTask<CountersDetail> GetCountersAsync(TOperationContext context, string docId, StringValues counters, bool full);

        public override async ValueTask ExecuteAsync()
        {
            var docId = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("docId");
            var full = RequestHandler.GetBoolValueQueryString("full", required: false) ?? false;
            var counters = RequestHandler.GetStringValuesQueryString("counter", required: false);

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var countersDetail = await GetCountersAsync(context, docId, counters, full);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, countersDetail.ToJson());
                }
            }
        }
    }
}
