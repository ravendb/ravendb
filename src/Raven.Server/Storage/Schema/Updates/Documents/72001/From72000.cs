namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From72000 : ISchemaUpdate
    {
        public int From => 72_000;
        public int To => 72_001;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        // Metadata-only sentinel bump (lazy mixed-mode; no upgrade-time data rewrite).
        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
