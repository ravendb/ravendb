using System.Collections.Generic;
using Sparrow;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlRunStats
    {
        public readonly Dictionary<EtlItemType, int> NumberOfExtractedItems = new Dictionary<EtlItemType, int>()
        {
            {EtlItemType.Document, 0},
            {EtlItemType.CounterGroup, 0},
            {EtlItemType.TimeSeries, 0},
        };

        public readonly Dictionary<EtlItemType, int> NumberOfTransformedItems = new Dictionary<EtlItemType, int>()
        {
            {EtlItemType.Document, 0},
            {EtlItemType.CounterGroup, 0},
            {EtlItemType.TimeSeries, 0},
        };

        public readonly Dictionary<EtlItemType, int> NumberOfTransformedTombstones = new Dictionary<EtlItemType, int>()
        {
            {EtlItemType.Document, 0},
            {EtlItemType.CounterGroup, 0},
            {EtlItemType.TimeSeries, 0},
        };

        public readonly Dictionary<EtlItemType, long> LastExtractedEtags = new Dictionary<EtlItemType, long>
        {
            {EtlItemType.Document, 0},
            {EtlItemType.CounterGroup, 0},
            {EtlItemType.TimeSeries, 0},
        };

        public readonly Dictionary<EtlItemType, long> LastTransformedEtags = new Dictionary<EtlItemType, long>
        {
            {EtlItemType.Document, 0},
            {EtlItemType.CounterGroup, 0},
            {EtlItemType.TimeSeries, 0},
        };
        
        public readonly Dictionary<EtlItemType, long> LastFilteredOutEtags = new Dictionary<EtlItemType, long>
        {
            {EtlItemType.Document, 0},
            {EtlItemType.CounterGroup, 0},
            {EtlItemType.TimeSeries, 0},
        };

        public Size CurrentlyAllocated;

        public long LastLoadedEtag;

        public int NumberOfLoadedItems;

        public int TransformationErrorCount;

        public string BatchCompleteReason;

        public string ChangeVector;

        public bool? SuccessfullyLoaded;

        public Size BatchSize;
    }
}
