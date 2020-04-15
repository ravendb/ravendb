
namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public unsafe class From40015 : ISchemaUpdate
    {
        public int From => 40_015;
        public int To => 41_016;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
