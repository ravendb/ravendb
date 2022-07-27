using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexProcessorForGenerateCSharpIndexDefinition<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<string, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexProcessorForGenerateCSharpIndexDefinition([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<string> CreateCommandForNode(string nodeTag)
    {
        var name = GetName();

        return new GenerateCSharpIndexDefinitionCommand(name, nodeTag);
    }

    protected string GetName() => RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
}
