using System.Collections.Generic;
using Voron.Data.BTrees;

namespace Raven.Server.Indexing
{
    public class VoronStreamCache
    {
        public Dictionary<string, Tree.ChunkDetails[]> ChunksByName = new Dictionary<string, Tree.ChunkDetails[]>();
    }
}
