using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal sealed class EtlHandlerProcessorForDeleteErrors : AbstractTaskErrorsHandlerProcessorForDeleteErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForDeleteErrors(DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TaskCategory TaskCategory => TaskCategory.Etl;

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var names = GetTaskNames();

        foreach (var name in names)
            RequestHandler.Database.TaskErrorsStorage.DeleteErrorsOfTask(name, TaskCategory);

        return ValueTask.CompletedTask;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
