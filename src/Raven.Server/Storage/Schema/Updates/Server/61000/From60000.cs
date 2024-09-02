namespace Raven.Server.Storage.Schema.Updates.Server
{
    public class From60000 : ISchemaUpdate
    {
        public int From => 60_000;
        public int To => 61_000;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
