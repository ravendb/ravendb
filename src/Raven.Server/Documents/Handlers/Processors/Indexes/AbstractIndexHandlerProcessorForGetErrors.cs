using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForGetErrors<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<IndexErrors[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForGetErrors([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<IndexErrors[]> CreateCommandForNode(string nodeTag) => new GetIndexErrorsOperation.GetIndexErrorsCommand(GetIndexNames(), nodeTag);

    protected string[] GetIndexNames()
    {
        return RequestHandler.GetStringValuesQueryString("name", required: false);
    }
}
