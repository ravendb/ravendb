using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForStale : AbstractIndexHandlerProcessorForStale<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForStale([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask<GetIndexStalenessCommand.IndexStaleness> GetResultForCurrentNodeAsync()
    {
        var name = GetName();

        var index = RequestHandler.Database.IndexStore.GetIndex(name);
        if (index == null)
            IndexDoesNotExistException.ThrowFor(name);

        using (var context = QueryOperationContext.Allocate(RequestHandler.Database, index))
        using (context.OpenReadTransaction())
        {
            var stalenessReasons = new List<string>();
            var isStale = index.IsStale(context, stalenessReasons: stalenessReasons);

            return ValueTask.FromResult(new GetIndexStalenessCommand.IndexStaleness
            {
                IsStale = isStale,
                StalenessReasons = stalenessReasons
            });
        }
    }

    protected override Task<GetIndexStalenessCommand.IndexStaleness> GetResultForRemoteNodeAsync(RavenCommand<GetIndexStalenessCommand.IndexStaleness> command) => RequestHandler.ExecuteRemoteAsync(command);
}
