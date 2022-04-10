using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForProgress<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<IndexProgress[], TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForProgress([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected override RavenCommand<IndexProgress[]> CreateCommandForNode(string nodeTag) => new GetIndexesProgressCommand(nodeTag);
}
