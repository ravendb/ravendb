using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public class EmbeddedCollection : CollectionWithReferences, ICollectionReference
    {
        public List<string> Columns { get; set; }
        public RelationType Type { get; set; }
        
        public EmbeddedCollection(string sourceTableSchema, string sourceTableName, RelationType type, List<string> columns, string name) : base(sourceTableSchema, sourceTableName, name)
        {
            Columns = columns;
            Type = type;
        }
    }
}
