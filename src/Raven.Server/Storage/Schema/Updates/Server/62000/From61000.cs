namespace Raven.Server.Storage.Schema.Updates.Server
{
    public class From61000 : ISchemaUpdate
    {
        public int From => 61_000;
        public int To => 62_000;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
