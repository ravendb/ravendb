using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors;

internal class IndexHandlerProcessorForGetAll : AbstractIndexHandlerProcessorForGetAll<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForGetAll([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask<IndexDefinition[]> GetResultForCurrentNodeAsync()
    {
        var name = GetName();

        var indexDefinitions = GetIndexDefinitions(RequestHandler, name);

        return ValueTask.FromResult(indexDefinitions);
    }

    protected override Task<IndexDefinition[]> GetResultForRemoteNodeAsync(RavenCommand<IndexDefinition[]> command) => RequestHandler.ExecuteRemoteAsync(command);

    internal static IndexDefinition[] GetIndexDefinitions(DatabaseRequestHandler requestHandler, string indexName)
    {
        var start = requestHandler.GetStart();
        var pageSize = requestHandler.GetPageSize();

        IndexDefinition[] indexDefinitions;
        if (string.IsNullOrEmpty(indexName))
            indexDefinitions = requestHandler.Database.IndexStore
                .GetIndexes()
                .OrderBy(x => x.Name)
                .Skip(start)
                .Take(pageSize)
                .Select(x => x.GetIndexDefinition())
                .ToArray();
        else
        {
            var index = requestHandler.Database.IndexStore.GetIndex(indexName);
            if (index == null)
                return null;

            indexDefinitions = new[] { index.GetIndexDefinition() };
        }

        return indexDefinitions;
    }
}
