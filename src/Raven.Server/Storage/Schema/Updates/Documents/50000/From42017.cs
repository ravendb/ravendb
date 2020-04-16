namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public unsafe class From42017 : ISchemaUpdate
    {
        public int From => 42_017;

        public int To => 50_000;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
