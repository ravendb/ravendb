namespace Raven.Server.Storage.Schema.Updates.LuceneIndex
{
    public class From60000 : ISchemaUpdate
    {
        public int From => 60_000;
        public int To => 61_000;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.LuceneIndex;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
