using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal sealed class IndexHandlerProcessorForGetAll : AbstractIndexHandlerProcessorForGetAll<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForGetAll([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override IndexDefinition[] GetIndexDefinitions(string indexName, int start, int pageSize) => GetIndexDefinitions(RequestHandler, indexName, start, pageSize);

    protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexDefinition[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    internal static IndexDefinition[] GetIndexDefinitions(DatabaseRequestHandler requestHandler, string indexName, int start, int pageSize)
    {
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
