using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal class EtlHandlerProcessorForDebugStats : AbstractEtlHandlerProcessorForDebugStats<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForDebugStats([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var names = GetNames();
        var debugStats = EtlHandlerProcessorForStats.GetProcessesToReportOn(RequestHandler.Database, names)
            .Select(x => new EtlTaskDebugStats
            {
                TaskName = x.Key,
                Stats = x.Value.Select(y => new EtlProcessTransformationDebugStats
                {
                    TransformationName = y.TransformationName,
                    Statistics = y.Statistics,
                    Metrics = y.Metrics
                }).ToArray()
            }).ToArray();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, "Results", debugStats, (w, c, stats) => w.WriteObject(c.ReadObject(stats.ToJson(), "etl/debug/stats")));
                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlTaskDebugStats[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
