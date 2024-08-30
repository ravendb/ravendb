using System;

namespace Raven.Server.Storage.Schema.Updates.CoraxIndex;

public sealed class From60000 : ISchemaUpdate
{
    public int From => 60_000;

    public int To => 61_000;

    public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.CoraxIndex;

    public bool Update(UpdateStep step)
    {
        return true;
    }
}
