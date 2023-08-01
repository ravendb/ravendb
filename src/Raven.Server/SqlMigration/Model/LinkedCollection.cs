using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public sealed class LinkedCollection : AbstractCollection, ICollectionReference
    {
        public List<string> JoinColumns { get; set; }
        public RelationType Type { get; set; }
        
        public LinkedCollection(string sourceTableSchema, string sourceTableName, RelationType type, List<string> joinColumns, string name) : base(sourceTableSchema, sourceTableName, name)
        {
            JoinColumns = joinColumns;
            Type = type;
        }
    }
}
