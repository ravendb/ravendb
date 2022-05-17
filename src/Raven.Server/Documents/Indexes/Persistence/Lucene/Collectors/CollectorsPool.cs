using System.Collections.Generic;
using Lucene.Net.Search;
using Sparrow;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors
{
    internal static class CollectorsPool
    {
        public static ObjectPool<List<ScoreDoc>> Instance = new ObjectPool<List<ScoreDoc>>(() => new List<ScoreDoc>());
    }
}
