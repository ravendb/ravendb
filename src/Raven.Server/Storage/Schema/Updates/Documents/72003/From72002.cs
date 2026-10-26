namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From72002 : ISchemaUpdate
    {
        public int From => 72_002;
        public int To => 72_003;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        // Sentinel bump, nothing to rewrite. A 72_002 server writes revisions without maintaining the conflict
        // revisions tree, and refuses to open storage whose schema version is above its own.
        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
