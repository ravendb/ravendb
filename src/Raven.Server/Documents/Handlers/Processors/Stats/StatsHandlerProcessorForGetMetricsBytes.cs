using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal sealed class StatsHandlerProcessorForGetMetricsBytes : AbstractStatsHandlerProcessorForGetMetricsBytes<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StatsHandlerProcessorForGetMetricsBytes([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            var empty = RequestHandler.GetBoolValueQueryString("empty", required: false) ?? true;

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var metrics = RequestHandler.Database.Metrics.ToJsonForGetBytes(empty);
                context.Write(writer, metrics);
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
