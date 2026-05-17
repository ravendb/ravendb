using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal abstract class AbstractTaskErrorsHandlerProcessorForDeleteErrors<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractTaskErrorsHandlerProcessorForDeleteErrors([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract TaskCategory TaskCategory { get; }

    protected override RavenCommand<object> CreateCommandForNode(string nodeTag)
    {
        var names = GetTaskNames();

        return new DeleteTaskErrorsCommand(names, TaskCategory, nodeTag);
    }

    protected StringValues GetTaskNames() => RequestHandler.GetStringValuesQueryString("name", required: true);
}
