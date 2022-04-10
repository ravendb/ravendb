using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioIndexHandlerProcessorForGetIndexErrorsCount<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<GetIndexErrorsCountCommand.IndexErrorsCount[], TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractStudioIndexHandlerProcessorForGetIndexErrorsCount([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected override RavenCommand<GetIndexErrorsCountCommand.IndexErrorsCount[]> CreateCommandForNode(string nodeTag) => new GetIndexErrorsCountCommand(GetIndexNames(), nodeTag);

    protected string[] GetIndexNames()
    {
        return RequestHandler.GetStringValuesQueryString("name", required: false);
    }
}
