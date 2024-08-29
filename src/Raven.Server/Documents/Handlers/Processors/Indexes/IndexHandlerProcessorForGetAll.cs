using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
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

    protected override IndexDefinition[] GetIndexDefinitions(StringValues indexNames, int start, int pageSize) => GetIndexDefinitions(RequestHandler, indexNames, start, pageSize);

    protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexDefinition[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    internal static IndexDefinition[] GetIndexDefinitions(DatabaseRequestHandler requestHandler, StringValues indexNames, int start, int pageSize)
    {
        IndexDefinition[] indexDefinitions;
        switch (indexNames.Count)
        {
            case 0:
                indexDefinitions = requestHandler.Database.IndexStore
                    .GetIndexes()
                    .OrderBy(x => x.Name)
                    .Skip(start)
                    .Take(pageSize)
                    .Select(x => x.GetIndexDefinition())
                    .ToArray();
                break;
            case 1:
                {
                    var index = requestHandler.Database.IndexStore.GetIndex(indexNames);
                    if (index == null)
                        return null;

                    indexDefinitions = new[] { index.GetIndexDefinition() };
                    break;
                }
            default:
                indexDefinitions = requestHandler.Database.IndexStore
                    .GetIndexes()
                    .Where(x => indexNames.Contains(x.Name))
                    .OrderBy(x => x.Name)
                    .Select(x => x.GetIndexDefinition())
                    .ToArray();
                break;
        }

        return indexDefinitions;
    }
}
