using System;
using System.Collections.Generic;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    public sealed class CollectionStatistics
    {
        public CollectionStatistics()
        {
            Collections = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        }

        public long CountOfDocuments { get; set; }
        public long CountOfConflicts { get; set; }

        public Dictionary<string, long> Collections { get; set; }

        public DynamicJsonValue ToJson()
        {
            DynamicJsonValue collections = new DynamicJsonValue();

            DynamicJsonValue stats = new DynamicJsonValue()
            {
                [nameof(CollectionStatistics.CountOfDocuments)] = CountOfDocuments,
                [nameof(CollectionStatistics.CountOfConflicts)] = CountOfConflicts,
                [nameof(CollectionStatistics.Collections)] = collections
            };

            foreach (var collection in Collections)
            {
                collections[collection.Key] = collection.Value;
            }

            return stats;
        }
    }

    public sealed class DetailedCollectionStatistics
    {
        public DetailedCollectionStatistics()
        {
            Collections = new Dictionary<string, CollectionDetails>(StringComparer.OrdinalIgnoreCase);
        }

        public long CountOfDocuments { get; set; }
        public long CountOfConflicts { get; set; }

        public Dictionary<string, CollectionDetails> Collections { get; set; }

        public DynamicJsonValue ToJson()
        {
            DynamicJsonValue collections = new DynamicJsonValue();

            DynamicJsonValue stats = new DynamicJsonValue()
            {
                [nameof(CollectionStatistics.CountOfDocuments)] = CountOfDocuments,
                [nameof(CollectionStatistics.CountOfConflicts)] = CountOfConflicts,
                [nameof(CollectionStatistics.Collections)] = collections
            };

            foreach (var collection in Collections)
            {
                collections[collection.Key] = collection.Value.ToJson();
            }

            return stats;

        }
    }

    public sealed class CollectionDetails : IDynamicJson
    {
        public string Name { get; set; }
        public long CountOfDocuments { get; set; }
        public Size Size { get; set; }
        public Size DocumentsSize { get; set; }
        public Size TombstonesSize { get; set; }
        public Size RevisionsSize { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(CountOfDocuments)] = CountOfDocuments,
                [nameof(Size)] = new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = Size.HumaneSize,
                    [nameof(Size.SizeInBytes)] = Size.SizeInBytes
                },
                [nameof(DocumentsSize)] = new DynamicJsonValue
                {
                    [nameof(DocumentsSize.HumaneSize)] = DocumentsSize.HumaneSize,
                    [nameof(DocumentsSize.SizeInBytes)] = DocumentsSize.SizeInBytes
                },
                [nameof(TombstonesSize)] = new DynamicJsonValue
                {
                    [nameof(TombstonesSize.HumaneSize)] = TombstonesSize.HumaneSize,
                    [nameof(TombstonesSize.SizeInBytes)] = TombstonesSize.SizeInBytes
                },
                [nameof(RevisionsSize)] = new DynamicJsonValue
                {
                    [nameof(RevisionsSize.HumaneSize)] = RevisionsSize.HumaneSize,
                    [nameof(RevisionsSize.SizeInBytes)] = RevisionsSize.SizeInBytes
                }
            };
        }
    }
}
