using System;
using JetBrains.Annotations;
using Raven.Server.Config;

namespace Raven.Server.Documents.Indexes;

public class DatabaseIndexHasChangedController : AbstractIndexHasChangedController
{
    private readonly DocumentDatabase _database;

    public DatabaseIndexHasChangedController([NotNull] DocumentDatabase database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override IndexInformationHolder GetIndex(string name)
    {
        var index = _database.IndexStore.GetIndex(name);
        if (index == null)
            return null;

        return index.ToIndexInformationHolder();
    }

    protected override RavenConfiguration GetDatabaseConfiguration() => _database.Configuration;
}
