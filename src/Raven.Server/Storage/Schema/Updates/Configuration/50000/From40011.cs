namespace Raven.Server.Storage.Schema.Updates.Configuration
{
    public unsafe class From40011 : ISchemaUpdate
    {
        public int From => 40_011;

        public int To => 50_000;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Configuration;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
