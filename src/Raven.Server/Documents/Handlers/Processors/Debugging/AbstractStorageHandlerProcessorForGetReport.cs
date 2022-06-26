using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Storage;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Debugging;

internal abstract class AbstractStorageHandlerProcessorForGetReport<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractStorageHandlerProcessorForGetReport([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<object> CreateCommandForNode(string nodeTag) => new GetStorageReportCommand(nodeTag);
}
