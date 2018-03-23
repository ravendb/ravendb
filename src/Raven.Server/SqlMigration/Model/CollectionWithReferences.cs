using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public class CollectionWithReferences : AbstractCollection
    {
        public List<EmbeddedCollection> NestedCollections { get; set; }
        public List<LinkedCollection> LinkedCollections { get; set; }

        public CollectionWithReferences(string sourceTableSchema, string sourceTableName, string name) : base(sourceTableSchema, sourceTableName, name)
        {
        }
    }
}
