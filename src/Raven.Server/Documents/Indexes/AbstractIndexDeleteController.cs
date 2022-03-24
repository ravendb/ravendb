using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;

namespace Raven.Server.Documents.Indexes;

public abstract class AbstractIndexDeleteController
{
    protected readonly ServerStore ServerStore;

    protected AbstractIndexDeleteController([NotNull] ServerStore serverStore)
    {
        ServerStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
    }

    protected abstract string GetDatabaseName();

    protected abstract IndexDefinitionBaseServerSide GetIndexDefinition(string name);

    protected abstract ValueTask CreateIndexAsync(IndexDefinition definition, string raftRequestId);

    protected abstract ValueTask WaitForIndexNotificationAsync(long index);

    public async ValueTask<bool> TryDeleteIndexIfExistsAsync(string name, string raftRequestId)
    {
        var indexDefinition = GetIndexDefinition(name);
        if (indexDefinition == null)
            return false;

        if (name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix))
        {
            await HandleSideBySideIndexDeleteAsync(name, raftRequestId);
            return true;
        }

        var (index, _) = await ServerStore.SendToLeaderAsync(new DeleteIndexCommand(indexDefinition.Name, GetDatabaseName(), raftRequestId));

        await WaitForIndexNotificationAsync(index);

        return true;
    }

    private async ValueTask HandleSideBySideIndexDeleteAsync(string name, string raftRequestId)
    {
        var originalIndexName = name.Remove(0, Constants.Documents.Indexing.SideBySideIndexNamePrefix.Length);
        var originalIndexDefinition = (MapIndexDefinition)GetIndexDefinition(originalIndexName); // tis is OK, map-reduce one inherits from map
        if (originalIndexDefinition == null)
        {
            // we cannot find the original index
            // but we need to remove the side by side one by the original name
            var (index, _) = await ServerStore.SendToLeaderAsync(new DeleteIndexCommand(originalIndexName, GetDatabaseName(), raftRequestId));

            await WaitForIndexNotificationAsync(index);

            return;
        }

        // deleting the side by side index means that we need to save the original one in the database record

        var indexDefinition = originalIndexDefinition.IndexDefinition;
        indexDefinition.Name = originalIndexName;
        await CreateIndexAsync(indexDefinition, raftRequestId);
    }
}
