using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes;

public class DatabaseIndexCreateController : AbstractIndexCreateController
{
    private readonly DocumentDatabase _database;

    public DatabaseIndexCreateController([NotNull] DocumentDatabase database)
        : base(database.ServerStore)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override string GetDatabaseName() => _database.Name;

    public override SystemTime GetDatabaseTime() => _database.Time;

    public override RavenConfiguration GetDatabaseConfiguration() => _database.Configuration;

    protected override IndexInformationHolder GetIndex(string name)
    {
        var index = _database.IndexStore.GetIndex(name);
        if (index == null)
            return null;

        return index.ToIndexInformationHolder();
    }

    protected override IEnumerable<string> GetIndexNames()
    {
        foreach (var index in _database.IndexStore.GetIndexes())
            yield return index.Name;
    }

    protected override ValueTask<long> GetCollectionCountAsync(string collection)
    {
        using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
            return ValueTask.FromResult(_database.DocumentsStorage.GetCollection(collection, context).Count);
    }

    protected override IEnumerable<IndexInformationHolder> GetIndexes()
    {
        foreach (var index in _database.IndexStore.GetIndexes())
            yield return index.ToIndexInformationHolder();
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index, TimeSpan? timeout = null)
    {
        await _database.RachisLogIndexNotifications.WaitForIndexNotification(index, timeout ?? _database.ServerStore.Engine.OperationTimeout);
    }
}
