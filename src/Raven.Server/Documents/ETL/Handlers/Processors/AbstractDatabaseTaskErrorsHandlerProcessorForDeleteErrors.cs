using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

// Shared current-node/proxy body for the per-category "delete task errors" endpoints (ETL, AI, CDC Sink)
// on a non-sharded database. Subclasses supply only the task category.
internal abstract class AbstractDatabaseTaskErrorsHandlerProcessorForDeleteErrors : AbstractTaskErrorsHandlerProcessorForDeleteErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    protected AbstractDatabaseTaskErrorsHandlerProcessorForDeleteErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        foreach (var name in GetTaskNames())
            RequestHandler.Database.TaskErrorsStorage.DeleteErrorsOfTask(name, TaskCategory);

        return ValueTask.CompletedTask;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
