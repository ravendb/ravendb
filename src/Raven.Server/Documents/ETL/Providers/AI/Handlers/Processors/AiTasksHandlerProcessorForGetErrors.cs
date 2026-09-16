using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal sealed class AiTasksHandlerProcessorForGetErrors : AbstractDatabaseTaskErrorsHandlerProcessorForGetErrors
{
    public AiTasksHandlerProcessorForGetErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TaskCategory TaskCategory => TaskCategory.Ai;

    protected override string EndpointDebugName => "ai/errors";
}
