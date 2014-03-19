using System.Collections.Generic;

namespace Raven.Database.Indexing.IndexMerging
{
    public class MergeProposal
    {

        public List<IndexData> ProposedForMerge = new List<IndexData>();
        public IndexData MergedData { get; set; }

        public string IndexMergeSuggestion { get; set; }


    }
}