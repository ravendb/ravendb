using System.Diagnostics.CodeAnalysis;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal abstract class AbstractTaskErrorsHandlerProcessorForGetAllErrors<TRequestHandler, TOperationContext> : AbstractTaskErrorsHandlerProcessorBase<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractTaskErrorsHandlerProcessorForGetAllErrors([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<TaskErrors[]> CreateCommandForNode(string nodeTag)
    {
        return new GetAllTaskErrorsCommand(nodeTag);
    }
}
