namespace Raven.Server.Storage.Schema.Updates.LuceneIndex
{
    public unsafe class From54000 : ISchemaUpdate
    {
        public int From => 54_000;
        public int To => 60_000;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.LuceneIndex;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
