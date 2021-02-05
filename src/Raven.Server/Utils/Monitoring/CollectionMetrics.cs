// -----------------------------------------------------------------------
//  <copyright file="CollectionMetrics.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Monitoring
{
    public class CollectionMetrics
    {
        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        
        public long DocumentsCount { get; set; }
        
        public long TotalSizeInBytes { get; set; }
        public long DocumentsSizeInBytes { get; set; }
        public long TombstonesSizeInBytes { get; set; }
        public long RevisionsSizeInBytes { get; set; }

        public CollectionMetrics(string databaseName, CollectionDetails collectionDetails)
        {
            DatabaseName = databaseName;
            CollectionName = collectionDetails.Name;
            DocumentsCount = collectionDetails.CountOfDocuments;
            TotalSizeInBytes = collectionDetails.Size.SizeInBytes;
            DocumentsSizeInBytes = collectionDetails.DocumentsSize.SizeInBytes;
            TombstonesSizeInBytes = collectionDetails.TombstonesSize.SizeInBytes;
            RevisionsSizeInBytes = collectionDetails.RevisionsSize.SizeInBytes;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseName)] = DatabaseName,
                [nameof(CollectionName)] = CollectionName,
                [nameof(DocumentsCount)] = DocumentsCount,
                [nameof(TotalSizeInBytes)] = TotalSizeInBytes,
                [nameof(DocumentsSizeInBytes)] = DocumentsSizeInBytes,
                [nameof(TombstonesSizeInBytes)] = TombstonesSizeInBytes,
                [nameof(RevisionsSizeInBytes)] = RevisionsSizeInBytes
            };
        }
    }
    
    public class CollectionsMetrics
    {
        public List<CollectionMetrics> Results { get; set; } = new List<CollectionMetrics>();
        
        public string PublicServerUrl { get; set; }
        
        public string NodeTag { get; set; }
    }
}
