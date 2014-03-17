using System.Collections.Generic;

namespace Raven.Abstractions.Indexing
{
    public class MergeSuggestions
    {
        public List<string> CanMerge = new List<string>();  // index names

        public IndexDefinition MergedIndex = new IndexDefinition();  //propose for new index with all it's properties
    }
}