//-----------------------------------------------------------------------
// <copyright file="IndexStats.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Data.Indexes
{
    public static class IndexingOperation
    {
        public static class Lucene
        {
            public const string Delete = "Lucene/Delete";
            public const string Convert = "Lucene/Convert";
            public const string AddDocument = "Lucene/AddDocument";
            public const string FlushToDisk = "Lucene/FlushToDisk";
            public const string RecreateSearcher = "Lucene/RecreateSearcher";
        }

        public static class Storage
        {
            public const string Commit = "Storage/Commit";
        }

        public static class Reduce
        {
            public const string TreeScope = "Tree";
            public const string NestedValuesScope = "NestedValues";
            public const string LeafAggregation = "Aggregation/Leafs";
            public const string BranchAggregation = "Aggregation/Branches";
            public const string StoringReduceResult = "Storage/ReduceResult";
            public const string NestedValuesAggregation = "Aggregation/Values";
        }
    }
}
