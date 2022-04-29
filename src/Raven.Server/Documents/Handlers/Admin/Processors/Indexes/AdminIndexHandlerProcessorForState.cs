using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal class AdminIndexHandlerProcessorForState : AbstractAdminIndexHandlerProcessorForState<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminIndexHandlerProcessorForState(IndexState state, [NotNull] DatabaseRequestHandler requestHandler) : base(state, requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();
        var index = RequestHandler.Database.IndexStore.GetIndex(name);
        if (index == null)
            IndexDoesNotExistException.ThrowFor(name);

        switch (State)
        {
            case IndexState.Normal:
                index.Enable();
                break;
            case IndexState.Disabled:
                index.Disable();
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        return ValueTask.CompletedTask;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    protected override AbstractIndexStateController GetIndexStateProcessor() => RequestHandler.Database.IndexStore.State;
}
