using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public interface ICollectionReference
    {
        string SourceTableSchema { get; set; }
        
        string SourceTableName { get; set; }
        
        string Name { get; set; }
        
        List<string> JoinColumns { get; set; }
        
        RelationType Type { get; set; }
        
        Dictionary<string, string> ColumnsMapping { get; set; }
        
        Dictionary<string, string> AttachmentNameMapping { get; set; }
    }
}
