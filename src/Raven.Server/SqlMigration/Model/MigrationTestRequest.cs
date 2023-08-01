namespace Raven.Server.SqlMigration.Model
{
    public sealed class MigrationTestRequest
    {
        public SourceSqlDatabase Source { get; set; }
        public MigrationTestSettings Settings { get; set; }
    }
    
}
