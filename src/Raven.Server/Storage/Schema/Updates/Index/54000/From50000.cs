namespace Raven.Server.Storage.Schema.Updates.Index
{
    public unsafe class From5000 : ISchemaUpdate
    {
        public int From => 50_000;

        public int To => 54_000;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Index;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
