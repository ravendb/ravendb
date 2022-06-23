using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal class EtlHandlerProcessorForPerformance : AbstractEtlHandlerProcessorForPerformance<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForPerformance([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var names = GetNames();
        var stats = EtlHandlerProcessorForStats.GetProcessesToReportOn(RequestHandler.Database, names).Select(x => new EtlTaskPerformanceStats
        {
            TaskName = x.Key,
            TaskId = x.Value.First().TaskId, // since we grouped by task name it implies each task id inside group is the same
            EtlType = x.Value.First().EtlType,
            EtlSubType = x.Value.First().EtlSubType,
            Stats = x.Value.Select(y => new EtlProcessPerformanceStats
            {
                TransformationName = y.TransformationName,
                Performance = y.GetPerformanceStats()
            }).ToArray()
        }).ToArray();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteEtlTaskPerformanceStats(context, stats);
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlTaskPerformanceStats[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
