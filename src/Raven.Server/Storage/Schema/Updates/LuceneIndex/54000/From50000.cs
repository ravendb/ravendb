namespace Raven.Server.Storage.Schema.Updates.LuceneIndex
{
    public sealed unsafe class From5000 : ISchemaUpdate
    {
        public int From => 50_000;

        public int To => 54_000;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.LuceneIndex;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
