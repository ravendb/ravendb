namespace Raven.Server.Storage.Schema.Updates.Server
{
    public class From40010 : ISchemaUpdate
    {
        public int From => 40_010;
        public int To => 40_011;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            step.WriteTx.DeleteFixedTree("EtagIndexName");
            return true;
        }
    }
}
