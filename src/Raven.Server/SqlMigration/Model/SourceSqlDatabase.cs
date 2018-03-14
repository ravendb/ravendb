namespace Raven.Server.SqlMigration.Model
{
    public class SourceSqlDatabase
    {
        public MigrationProvider Provider { get; set; }
        public string ConnectionString { get; set;}
    }
}
