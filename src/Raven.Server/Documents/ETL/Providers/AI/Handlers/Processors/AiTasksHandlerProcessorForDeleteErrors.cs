using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal sealed class AiTasksHandlerProcessorForDeleteErrors : AbstractTaskErrorsHandlerProcessorForDeleteErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AiTasksHandlerProcessorForDeleteErrors(DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TaskCategory TaskCategory => TaskCategory.Ai;

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
