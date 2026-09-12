using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal sealed class AiTasksHandlerProcessorForDeleteErrors : AbstractDatabaseTaskErrorsHandlerProcessorForDeleteErrors
{
    public AiTasksHandlerProcessorForDeleteErrors(DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TaskCategory TaskCategory => TaskCategory.Ai;
}
