namespace Raven.Server.Storage.Schema.Updates.Server
{
    public unsafe class From42019 : ISchemaUpdate
    {
        public int From => 42_019;

        public int To => 50_000;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
