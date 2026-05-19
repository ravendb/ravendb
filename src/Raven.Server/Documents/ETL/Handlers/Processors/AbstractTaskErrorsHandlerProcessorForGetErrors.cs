using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal abstract class AbstractTaskErrorsHandlerProcessorForGetErrors<TRequestHandler, TOperationContext> : AbstractTaskErrorsHandlerProcessorBase<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractTaskErrorsHandlerProcessorForGetErrors([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract TaskCategory TaskCategory { get; }

    protected override RavenCommand<TaskErrors[]> CreateCommandForNode(string nodeTag)
    {
        var names = GetNames();

        return new GetTaskErrorsCommand(names, TaskCategory, nodeTag);
    }

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);
}
