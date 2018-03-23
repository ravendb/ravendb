using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public class LinkedCollection : AbstractCollection, ICollectionReference
    {
        public List<string> Columns { get; set; }
        
        public LinkedCollection(string sourceTableSchema, string sourceTableName, string name, List<string> columns) : base(sourceTableSchema, sourceTableName, name)
        {
            Columns = columns;
        }
    }
}
