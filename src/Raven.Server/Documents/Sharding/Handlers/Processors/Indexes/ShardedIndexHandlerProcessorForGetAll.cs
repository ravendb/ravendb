using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal sealed class ShardedIndexHandlerProcessorForGetAll : AbstractIndexHandlerProcessorForGetAll<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForGetAll([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override IndexDefinition[] GetIndexDefinitions(StringValues indexNames, int start, int pageSize)
    {
        IndexDefinition[] indexDefinitions;
        switch (indexNames.Count)
        {
            case 0:
                indexDefinitions = RequestHandler.DatabaseContext.Indexes
                    .GetIndexes()
                    .OrderBy(x => x.Name)
                    .Skip(start)
                    .Take(pageSize)
                    .Select(x => x.Definition.GetOrCreateIndexDefinitionInternal())
                    .ToArray();
                break;
            case 1:
                {
                    var index = RequestHandler.DatabaseContext.Indexes.GetIndex(indexNames);
                    if (index == null)
                        return null;

                    indexDefinitions = new[] { index.Definition.GetOrCreateIndexDefinitionInternal() };
                    break;
                }
            default:
                indexDefinitions = RequestHandler.DatabaseContext.Indexes
                    .GetIndexes()
                    .Where(x => indexNames.Contains(x.Name))
                    .OrderBy(x => x.Name)
                    .Select(x => x.Definition.GetOrCreateIndexDefinitionInternal())
                    .ToArray();
                break;
        }

        return indexDefinitions;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexDefinition[]> command, OperationCancelToken token)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
