namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From50002 : ISchemaUpdate
    {
        public int From => 50_002;
        public int To => 60_000;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
