using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Util;
using Raven.Server.Config;

namespace Raven.Server.Documents.Indexes;

public class DatabaseIndexCreateProcessor : AbstractIndexCreateProcessor
{
    private readonly DocumentDatabase _database;

    public DatabaseIndexCreateProcessor([NotNull] DocumentDatabase database)
        : base(database.ServerStore)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override string GetDatabaseName() => _database.Name;

    protected override SystemTime GetDatabaseTime() => _database.Time;

    protected override RavenConfiguration GetDatabaseConfiguration() => _database.Configuration;

    protected override IndexContext GetIndex(string name)
    {
        var index = _database.IndexStore.GetIndex(name);
        if (index == null)
            return null;

        return index.ToIndexContext();
    }

    protected override IEnumerable<string> GetIndexNames()
    {
        foreach (var index in _database.IndexStore.GetIndexes())
            yield return index.Name;
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await _database.RachisLogIndexNotifications.WaitForIndexNotification(index, _database.ServerStore.Engine.OperationTimeout);
    }
}
