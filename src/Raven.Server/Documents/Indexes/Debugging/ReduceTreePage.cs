using System.Collections.Generic;
using Sparrow.Json;
using Voron.Data.BTrees;

namespace Raven.Server.Documents.Indexes.Debugging
{
    public class ReduceTreePage
    {
        public ReduceTreePage()
        {
        }

        public ReduceTreePage(TreePage p)
        {
            Page = p;

            if (Page.IsLeaf)
                Entries = new List<MapResultInLeaf>(Page.NumberOfEntries);
            else
                Children = new List<ReduceTreePage>(Page.NumberOfEntries);
        }

        public TreePage Page { get; }

        public long PageNumber => Page.PageNumber;

        public bool IsLeaf => Page.IsLeaf;

        public bool IsBranch => Page.IsBranch;

        public readonly List<ReduceTreePage> Children;

        public readonly List<MapResultInLeaf> Entries;

        public BlittableJsonReaderObject AggregationResult;
    }
}