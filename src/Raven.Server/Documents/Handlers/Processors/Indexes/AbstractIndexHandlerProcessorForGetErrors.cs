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
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForGetErrors([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) 
        : base(requestHandler, contextPool)
    {
    }

    protected override RavenCommand<IndexErrors[]> CreateCommandForNode(string nodeTag) => new GetIndexErrorsOperation.GetIndexErrorsCommand(GetIndexNames(), nodeTag);

    protected string[] GetIndexNames()
    {
        return RequestHandler.GetStringValuesQueryString("name", required: false);
    }
}
