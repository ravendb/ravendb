using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForGetAllNames : AbstractIndexHandlerProcessorForGetAllNames<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForGetAllNames([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask<string[]> GetResultForCurrentNodeAsync()
    {
        var name = GetName();

        var indexDefinitions = IndexHandlerProcessorForGetAll.GetIndexDefinitions(RequestHandler, name);

        return indexDefinitions == null
            ? ValueTask.FromResult<string[]>(null)
            : ValueTask.FromResult(indexDefinitions.Select(x => x.Name).ToArray());
    }

    protected override Task<string[]> GetResultForRemoteNodeAsync(RavenCommand<string[]> command) => RequestHandler.ExecuteRemoteAsync(command);
}
