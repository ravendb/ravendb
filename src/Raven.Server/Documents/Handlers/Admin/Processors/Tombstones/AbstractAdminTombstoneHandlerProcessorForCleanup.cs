using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Tombstones;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Tombstones;

internal abstract class AbstractAdminTombstoneHandlerProcessorForCleanup<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<CleanupTombstonesCommand.Response, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminTombstoneHandlerProcessorForCleanup([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<CleanupTombstonesCommand.Response> CreateCommandForNode(string nodeTag) => new CleanupTombstonesCommand(nodeTag);
}
