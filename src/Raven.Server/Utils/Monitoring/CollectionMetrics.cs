// -----------------------------------------------------------------------
//  <copyright file="CollectionMetrics.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Monitoring
{
    public class CollectionMetrics
    {
        public string CollectionName { get; set; }
        
        public long DocumentsCount { get; set; }
        
        public long TotalSizeInBytes { get; set; }
        public long DocumentsSizeInBytes { get; set; }
        public long TombstonesSizeInBytes { get; set; }
        public long RevisionsSizeInBytes { get; set; }

        
        public CollectionMetrics()
        {
            // for deserialization
        }
        
        public CollectionMetrics(CollectionDetails collectionDetails)
        {
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
        public string PublicServerUrl { get; set; }
        public string NodeTag { get; set; }
        public List<PerDatabaseCollectionMetrics> Results { get; set; } = new List<PerDatabaseCollectionMetrics>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(PublicServerUrl)] = PublicServerUrl,
                [nameof(NodeTag)] = NodeTag,
                [nameof(Results)] = Results.Select(x => x.ToJson()).ToList()
            };
        }
    }
    
    public class PerDatabaseCollectionMetrics
    {
        public string DatabaseName { get; set; }
        public List<CollectionMetrics> Collections { get; set; } = new List<CollectionMetrics>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseName)] = DatabaseName,
                [nameof(Collections)] = Collections.Select(x => x.ToJson()).ToList()
            };
        }
    }
}
