using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.IndexMerging
{
    public class IndexMergeResults
    {
        public Dictionary<string, string> Unmergables = new Dictionary<string, string>(); // index name, reason
        public List<MergeSuggestions> Suggestions = new List<MergeSuggestions>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Unmergables)] = DynamicJsonValue.Convert(Unmergables),
                [nameof(Suggestions)] = new DynamicJsonArray(Suggestions.Select(x=>x.ToJson()))
            };
        }
    }

    public class MergeSuggestions
    {
        public IndexDefinition MergedIndex = new IndexDefinition();  //propose for new index with all it's properties

        // start MergedIndex != null
        public List<string> CanMerge = new List<string>();  // index names

        public string Collection = String.Empty; // the collection that is being merged
        // end MergedIndex != null

        // start MergedIndex == null
        public List<string> CanDelete = new List<string>();  // index names

        public string SurpassingIndex = string.Empty;
        // end MergedIndex == null

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CanMerge)] = new DynamicJsonArray(CanMerge),
                [nameof(CanDelete)] = new DynamicJsonArray(CanDelete),
                [nameof(MergedIndex)] = MergedIndex.ToJson(),
                [nameof(Collection)] = Collection,
                [nameof(SurpassingIndex)] = SurpassingIndex
            };
        }
    }

    internal class MergeProposal
    {
        public List<IndexData> ProposedForMerge = new List<IndexData>();
        public IndexData MergedData { get; set; }
        public string IndexMergeSuggestion { get; set; }
    }

}
