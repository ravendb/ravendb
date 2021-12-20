namespace Raven.Server.Storage.Schema.Updates.Server
{
    public class From50000 : ISchemaUpdate
    {
        public int From => 50_000;

        public int To => 53_000;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
