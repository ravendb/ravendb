using System;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public unsafe class From50000 : ISchemaUpdate
    {
        public int From => 50_000;
        public int To => 50_001;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        public bool Update(UpdateStep step)
        {
            throw new NotSupportedException($"Storage upgrade is not supported from schema version '{From}'. This schema was used during 5.0 Beta period and contains binary incompatible data. Please start the upgrade using data created by non-Beta build or export data from your 5.0 Beta server and import it to a 5.0 non-Beta build.");
        }
    }
}
