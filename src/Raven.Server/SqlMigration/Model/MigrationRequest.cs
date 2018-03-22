namespace Raven.Server.SqlMigration.Model
{
    public class MigrationRequest
    {
        public MigrationSettings Settings { get; set; }
        public SourceSqlDatabase Source { get; set; }
    }
}
