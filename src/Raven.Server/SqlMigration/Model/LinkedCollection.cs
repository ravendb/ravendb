using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public class LinkedCollection : AbstractCollection, ICollectionReference
    {
        public List<string> Columns { get; set; }
        public RelationType Type { get; set; }
        
        public LinkedCollection(string sourceTableSchema, string sourceTableName, RelationType type, List<string> columns, string name) : base(sourceTableSchema, sourceTableName, name)
        {
            Columns = columns;
            Type = type;
        }
    }
}
