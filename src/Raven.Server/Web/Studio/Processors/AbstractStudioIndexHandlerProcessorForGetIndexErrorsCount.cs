using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioIndexHandlerProcessorForGetIndexErrorsCount<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<GetIndexErrorsCountCommand.IndexErrorsCount[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractStudioIndexHandlerProcessorForGetIndexErrorsCount([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<GetIndexErrorsCountCommand.IndexErrorsCount[]> CreateCommandForNode(string nodeTag) => new GetIndexErrorsCountCommand(GetIndexNames(), nodeTag);

    protected string[] GetIndexNames()
    {
        return RequestHandler.GetStringValuesQueryString("name", required: false);
    }
}
