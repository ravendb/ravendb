using System;

namespace Raven.Server.Storage.Schema.Updates.CoraxIndex;

public sealed class From61000 : ISchemaUpdate
{
    public int From => 61_000;

    public int To => 62_000;

    public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.CoraxIndex;

    public bool Update(UpdateStep step)
    {
        return true;
    }
}
