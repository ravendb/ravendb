using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Tombstones;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Tombstones;

internal abstract class AbstractAdminTombstoneHandlerProcessorForState<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<GetTombstonesStateCommand.Response, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminTombstoneHandlerProcessorForState([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<GetTombstonesStateCommand.Response> CreateCommandForNode(string nodeTag) => new GetTombstonesStateCommand(nodeTag);
}
