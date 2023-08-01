namespace Raven.Server.SqlMigration.Model
{
    public sealed class MigrationRequest
    {
        public MigrationSettings Settings { get; set; }
        public SourceSqlDatabase Source { get; set; }
    }
}
