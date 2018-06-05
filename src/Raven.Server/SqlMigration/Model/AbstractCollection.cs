using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public abstract class AbstractCollection
    {
        // SQL Schema name
        public string SourceTableSchema { get; set; }
        
        // SQL Table name
        public string SourceTableName { get; set; }
        
        // RavenDB Collection/Property name
        public string Name { get; set; }
        
        // SQL Column Name -> Document Id Property Name
        public Dictionary<string, string> ColumnsMapping { get; set; }
        
        // SQL Column Name -> Attachment Name
        public Dictionary<string, string> AttachmentNameMapping { get; set; }


        protected AbstractCollection(string sourceTableSchema, string sourceTableName, string name)
        {
            SourceTableSchema = sourceTableSchema;
            SourceTableName = sourceTableName;
            Name = name;
        }
    }
}
