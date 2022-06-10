using System;
using System.Collections.Generic;
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

internal class EtlHandlerProcessorForProgress : AbstractEtlHandlerProcessorForProgress<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForProgress([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        using (context.OpenReadTransaction())
        {
            var names = GetNames();
            var performance = EtlHandlerProcessorForStats.GetProcessesToReportOn(RequestHandler.Database, names).Select(x => new EtlTaskProgress
            {
                TaskName = x.Key,
                EtlType = x.Value.First().EtlType,
                ProcessesProgress = x.Value.Select(y => y.GetProgress(context)).ToArray()
            }).ToArray();

            writer.WriteEtlTaskProgress(context, performance);
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlTaskProgress[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
