using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public sealed class EmbeddedCollection : CollectionWithReferences, ICollectionReference
    {
        public List<string> JoinColumns { get; set; }
        public RelationType Type { get; set; }
        public EmbeddedDocumentSqlKeysStorage SqlKeysStorage { get; set; }
        
        public EmbeddedCollection(string sourceTableSchema, string sourceTableName, RelationType type, List<string> joinColumns, string name, EmbeddedDocumentSqlKeysStorage sqlKeysStorage = EmbeddedDocumentSqlKeysStorage.None) : base(sourceTableSchema, sourceTableName, name)
        {
            JoinColumns = joinColumns;
            Type = type;
            SqlKeysStorage = sqlKeysStorage;
        }
    }
}
