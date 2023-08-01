using System;

namespace Raven.Server.Storage.Schema.Updates.CoraxIndex;

public sealed unsafe class From54000 : ISchemaUpdate
{
    public int From => 54_000;

    public int To => 60_000;

    public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.CoraxIndex;

    public bool Update(UpdateStep step)
    {
        throw new NotSupportedException("Backward compatibility is not supported for Corax indexes built on versions before RavenDB 6.0. Please reset the index.");
    }
}
