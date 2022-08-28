using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal class EtlHandlerProcessorForStats : AbstractEtlHandlerProcessorForStats<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForStats([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var names = GetNames();
        var etlStats = GetProcessesToReportOn(RequestHandler.Database, names).Select(x => new EtlTaskStats
        {
            TaskName = x.Key,
            Stats = x.Value.Select(y => new EtlProcessTransformationStats
            {
                TransformationName = y.TransformationName,
                Statistics = y.Statistics
            }).ToArray()
        }).ToArray();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, "Results", etlStats, (w, c, stats) => w.WriteObject(context.ReadObject(stats.ToJson(), "etl/stats")));
                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlTaskStats[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    public static Dictionary<string, List<EtlProcess>> GetProcessesToReportOn(DocumentDatabase database, StringValues names)
    {
        Dictionary<string, List<EtlProcess>> etls;

        if (names.Count == 0)
            etls = database.EtlLoader.Processes
                .GroupBy(x => x.ConfigurationName)
                .OrderBy(x => x.Key)
                .ToDictionary(x => x.Key, x => x.OrderBy(y => y.TransformationName).ToList());
        else
        {
            etls = database.EtlLoader.Processes
                .Where(x => names.Contains(x.ConfigurationName, StringComparer.OrdinalIgnoreCase) || names.Contains(x.Name, StringComparer.OrdinalIgnoreCase))
                .GroupBy(x => x.ConfigurationName)
                .OrderBy(x => x.Key)
                .ToDictionary(x => x.Key, x => x.OrderBy(y => y.TransformationName).ToList());
        }

        return etls;
    }
}
