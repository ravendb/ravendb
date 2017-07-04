namespace Raven.Database.Server.Migrator
{
    public class MigrationDetails
    {
        public string Url { get; set; }

        public string ClusterToken { get; set; }

        public MigrationState MigrationState { get; set; }
    }
}