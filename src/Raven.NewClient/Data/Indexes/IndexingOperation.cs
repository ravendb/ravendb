//-----------------------------------------------------------------------
// <copyright file="IndexStats.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.NewClient.Data.Indexes
{
    public static class IndexingOperation
    {
        public const string LoadDocument = "LoadDocument";

        public static class Map
        {
            public const string DocumentRead = "Storage/DocumentRead";

            public const string Linq = "Linq";

            public const string Bloom = "Bloom";
        }

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
            public const string StoringReduceResult = "Storage/ReduceResults";
            public const string StoringNestedValues = "Storage/Values";
            public const string NestedValuesAggregation = "Aggregation/NestedValues";
            public const string NestedValuesRead = "Storage/Read";
            public const string BlittableJsonAggregation = "Aggregation/BlittableJson";
            public const string CreateBlittableJson = "CreateBlittableJson";
            public const string RemoveMapResult = "Storage/RemoveMapResult";
            public const string PutMapResult = "Storage/PutMapResult";
            public static string GetMapEntriesTree = "GetMapEntriesTree";
            public static string GetMapEntries = "GetMapEntries";
        }
    }
}
