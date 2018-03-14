using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public class CollectionWithReferences : AbstractCollection
    {
        public List<EmbeddedCollection> NestedCollections { get; set; }
        public List<LinkedCollection> LinkedCollections { get; set; }
    }
}
