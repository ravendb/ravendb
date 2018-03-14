namespace Raven.Server.SqlMigration.Model
{
    public abstract class AbstractCollection
    {
        // SQL Table name
        public string SourceTableName { get; set; }
        
        // RavenDB Collection name
        public string Name { get; set; }
    }
}
