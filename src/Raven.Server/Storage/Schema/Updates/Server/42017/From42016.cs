namespace Raven.Server.Storage.Schema.Updates.Server
{
    public class From42016 : ISchemaUpdate
    {
        public int From => 42_016;

        public int To => 42_017;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            return From42015.UpdateCertificatesTableInternal(step);
        }
    }
}
