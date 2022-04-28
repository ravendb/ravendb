using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Indexes;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal abstract class AbstractAdminIndexHandlerProcessorForState<TRequestHandler, TOperationContext> : AbstractHandlerProxyActionProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected readonly IndexState State;

    protected AbstractAdminIndexHandlerProcessorForState(IndexState state, [NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
        State = state;
    }

    protected abstract AbstractIndexStateController GetIndexStateProcessor();

    protected override RavenCommand CreateCommandForNode(string nodeTag)
    {
        var name = GetName();

        switch (State)
        {
            case IndexState.Normal:
                return new EnableIndexOperation.EnableIndexCommand(name, clusterWide: false);
            case IndexState.Disabled:
                return new DisableIndexOperation.DisableIndexCommand(name, clusterWide: false);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    protected string GetName()
    {
        return RequestHandler.GetStringQueryString("name");
    }

    public override async ValueTask ExecuteAsync()
    {
        var clusterWide = RequestHandler.GetBoolValueQueryString("clusterWide", false) ?? false;

        if (clusterWide)
        {
            var raftRequestId = RequestHandler.GetRaftRequestIdFromQuery();
            var name = GetName();

            var processor = GetIndexStateProcessor();
            await processor.SetStateAsync(name, State, $"{raftRequestId}/state");
            return;
        }

        await base.ExecuteAsync();
    }
}
