using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
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

    protected override IndexDefinition[] GetIndexDefinitions(string indexName, int start, int pageSize)
    {
        IndexDefinition[] indexDefinitions;
        if (string.IsNullOrEmpty(indexName))
            indexDefinitions = RequestHandler.DatabaseContext.Indexes
                .GetIndexes()
                .OrderBy(x => x.Name)
                .Skip(start)
                .Take(pageSize)
                .Select(x => x.Definition.GetOrCreateIndexDefinitionInternal())
                .ToArray();
        else
        {
            var index = RequestHandler.DatabaseContext.Indexes.GetIndex(indexName);
            if (index == null)
                return null;

            indexDefinitions = new[] { index.Definition.GetOrCreateIndexDefinitionInternal() };
        }

        return indexDefinitions;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexDefinition[]> command, OperationCancelToken token)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
