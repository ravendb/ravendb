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
        public long CountOfRevisionDocuments { get; set; }
        public long CountOfTombstones { get; set; }
        public long CountOfTimeSeriesDeletedRanges { get; set; }
        public long CountOfDocumentsConflicts { get; set; }
        public long CountOfAttachments { get; set; }
        public long CountOfCounterEntries { get; set; }
        public long CountOfTimeSeriesSegments { get; set; }

        public Dictionary<string, long> Collections { get; set; }

        public DynamicJsonValue ToJson()
        {
            DynamicJsonValue collections = new DynamicJsonValue();

            DynamicJsonValue stats = new DynamicJsonValue()
            {
                [nameof(CollectionStatistics.CountOfDocuments)] = CountOfDocuments,
                [nameof(CollectionStatistics.CountOfConflicts)] = CountOfConflicts,
                [nameof(CollectionStatistics.CountOfRevisionDocuments)] = CountOfRevisionDocuments,
                [nameof(CollectionStatistics.CountOfTombstones)] = CountOfTombstones,
                [nameof(CollectionStatistics.CountOfTimeSeriesDeletedRanges)] = CountOfTimeSeriesDeletedRanges,
                [nameof(CollectionStatistics.CountOfDocumentsConflicts)] = CountOfDocumentsConflicts,
                [nameof(CollectionStatistics.CountOfAttachments)] = CountOfAttachments,
                [nameof(CollectionStatistics.CountOfCounterEntries)] = CountOfCounterEntries,
                [nameof(CollectionStatistics.CountOfTimeSeriesSegments)] = CountOfTimeSeriesSegments,
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
        public long CountOfRevisionDocuments { get; set; }
        public long CountOfTombstones { get; set; }
        public long CountOfTimeSeriesDeletedRanges { get; set; }
        public long CountOfDocumentsConflicts { get; set; }
        public long CountOfAttachments { get; set; }
        public long CountOfCounterEntries { get; set; }
        public long CountOfTimeSeriesSegments { get; set; }

        public Dictionary<string, CollectionDetails> Collections { get; set; }

        public DynamicJsonValue ToJson()
        {
            DynamicJsonValue collections = new DynamicJsonValue();

            DynamicJsonValue stats = new DynamicJsonValue()
            {
                [nameof(CollectionStatistics.CountOfDocuments)] = CountOfDocuments,
                [nameof(CollectionStatistics.CountOfConflicts)] = CountOfConflicts,
                [nameof(CollectionStatistics.CountOfRevisionDocuments)] = CountOfRevisionDocuments,
                [nameof(CollectionStatistics.CountOfTombstones)] = CountOfTombstones,
                [nameof(CollectionStatistics.CountOfTimeSeriesDeletedRanges)] = CountOfTimeSeriesDeletedRanges,
                [nameof(CollectionStatistics.CountOfDocumentsConflicts)] = CountOfDocumentsConflicts,
                [nameof(CollectionStatistics.CountOfAttachments)] = CountOfAttachments,
                [nameof(CollectionStatistics.CountOfCounterEntries)] = CountOfCounterEntries,
                [nameof(CollectionStatistics.CountOfTimeSeriesSegments)] = CountOfTimeSeriesSegments,
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
        public long CountOfTombstones { get; set; }
        public long CountOfRevisions { get; set; }
        public long CountOfTimeSeriesDeletedRanges { get; set; }
        public long CountOfCounterEntries { get; set; }
        public long CountOfTimeSeriesSegments { get; set; }
        public Size TimeSeriesSegmentsSize { get; set; }
        public Size Size { get; set; }
        public Size DocumentsSize { get; set; }
        public Size TombstonesSize { get; set; }
        public Size RevisionsSize { get; set; }
        public Size TimeSeriesDeletedRangesSize { get; set; }
        public Size CounterEntriesSize { get; set; }

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
                [nameof(CountOfTombstones)] = CountOfTombstones,
                [nameof(TombstonesSize)] = new DynamicJsonValue
                {
                    [nameof(TombstonesSize.HumaneSize)] = TombstonesSize.HumaneSize,
                    [nameof(TombstonesSize.SizeInBytes)] = TombstonesSize.SizeInBytes
                },
                [nameof(CountOfRevisions)] = CountOfRevisions,
                [nameof(RevisionsSize)] = new DynamicJsonValue
                {
                    [nameof(RevisionsSize.HumaneSize)] = RevisionsSize.HumaneSize,
                    [nameof(RevisionsSize.SizeInBytes)] = RevisionsSize.SizeInBytes
                },
                [nameof(CountOfTimeSeriesDeletedRanges)] = CountOfTimeSeriesDeletedRanges,
                [nameof(TimeSeriesDeletedRangesSize)] = new DynamicJsonValue
                {
                    [nameof(TimeSeriesDeletedRangesSize.HumaneSize)] = TimeSeriesDeletedRangesSize.HumaneSize,
                    [nameof(TimeSeriesDeletedRangesSize.SizeInBytes)] = TimeSeriesDeletedRangesSize.SizeInBytes
                },
                [nameof(CountOfCounterEntries)] = CountOfCounterEntries,
                [nameof(CounterEntriesSize)] = new DynamicJsonValue
                {
                    [nameof(CounterEntriesSize.HumaneSize)] = CounterEntriesSize.HumaneSize,
                    [nameof(CounterEntriesSize.SizeInBytes)] = CounterEntriesSize.SizeInBytes
                },
                [nameof(CountOfTimeSeriesSegments)] = CountOfTimeSeriesSegments,
                [nameof(TimeSeriesSegmentsSize)] = new DynamicJsonValue
                {
                    [nameof(TimeSeriesSegmentsSize.HumaneSize)] = TimeSeriesSegmentsSize.HumaneSize,
                    [nameof(TimeSeriesSegmentsSize.SizeInBytes)] = TimeSeriesSegmentsSize.SizeInBytes
                }
            };
        }
    }
}
