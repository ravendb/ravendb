namespace Raven.Server.SqlMigration.Model
{
    public abstract class AbstractCollection
    {
        // SQL Schema name
        public string SourceTableSchema { get; set; }
        
        // SQL Table name
        public string SourceTableName { get; set; }
        
        // RavenDB Collection name
        public string Name { get; set; }


        protected AbstractCollection(string sourceTableSchema, string sourceTableName, string name)
        {
            SourceTableSchema = sourceTableSchema;
            SourceTableName = sourceTableName;
            Name = name;
        }
    }
}
