//-----------------------------------------------------------------------
// <copyright file="IndexingOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Indexes
{
    internal static class IndexingOperation
    {
        public const string LoadDocument = nameof(AbstractIndexCreationTask.LoadDocument);

        public const string LoadCompareExchangeValue = nameof(AbstractIndexCreationTask.LoadCompareExchangeValue);

        internal static class Map
        {
            public const string DocumentRead = "Storage/DocumentRead";

            public const string Linq = "Linq";

            public const string Jint = "Jint";

            public const string Bloom = "Bloom";
        }

        internal static class Lucene
        {
            public const string Delete = "Lucene/Delete";
            public const string Convert = "Lucene/Convert";
            public const string AddDocument = "Lucene/AddDocument";
            public const string Suggestion = "Lucene/Suggestion";
            public const string Commit = "Lucene/Commit";
            public const string Merge = "Lucene/Merge";
            public const string ApplyDeletes = "Lucene/ApplyDeletes";
            public const string RecreateSearcher = "Lucene/RecreateSearcher";
        }

        internal static class Storage
        {
            public const string Commit = "Storage/Commit";
        }

        internal static class Reduce
        {
            public const string TreeScope = "Tree";
            public const string NestedValuesScope = "NestedValues";
            public const string LeafAggregation = "Aggregation/Leafs";
            public const string BranchAggregation = "Aggregation/Branches";
            public const string StoringReduceResult = "Storage/ReduceResults";
            public const string NestedValuesAggregation = "Aggregation/NestedValues";
            public const string NestedValuesRead = "Storage/Read";
            public const string BlittableJsonAggregation = "Aggregation/BlittableJson";
            public const string CreateBlittableJson = "CreateBlittableJson";
            public const string RemoveMapResult = "Storage/RemoveMapResult";
            public const string PutMapResult = "Storage/PutMapResult";
            public static string GetMapEntriesTree = "GetMapEntriesTree";
            public static string GetMapEntries = "GetMapEntries";
            public static string SaveOutputDocuments = "SaveOutputDocuments";
            public static string DeleteOutputDocuments = "DeleteOutputDocuments";
        }

        internal static class Wait
        {
            public const string AcquireConcurrentlyRunningIndexesLock = "Wait/ConcurrentlyRunningIndexesLimit";
        }
    }
}
