using System.Collections.Generic;

namespace Raven.Client.Data.Collections
{
    public class CollectionStatistics
    {
        public CollectionStatistics()
        {
            Collections = new Dictionary<string, long>();
        }

        public int CountOfDocuments { get; set; }

        public Dictionary<string, long> Collections { get; set; }
    }
}