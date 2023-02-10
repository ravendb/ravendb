namespace Raven.Server.Storage.Schema.Updates.Server
{
    public class From53001 : ISchemaUpdate
    {
        public int From => 53_001;
        public int To => 60_000;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
