namespace Raven.Server.Storage.Schema.Updates.LuceneIndex
{
    public sealed unsafe class From40012 : ISchemaUpdate
    {
        public int From => 40_012;

        public int To => 50_000;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.LuceneIndex;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
