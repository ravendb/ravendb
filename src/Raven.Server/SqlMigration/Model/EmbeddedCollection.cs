using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public class EmbeddedCollection : CollectionWithReferences, ICollectionReference
    {
        public List<string> Columns { get; set; }
        
        public EmbeddedCollection(string sourceTableSchema, string sourceTableName, string name, List<string> columns) : base(sourceTableSchema, sourceTableName, name)
        {
            Columns = columns;
        }
    }
}
