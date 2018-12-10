using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlRunStats
    {
        public Dictionary<EtlItemType, int> NumberOfExtractedItems = new Dictionary<EtlItemType, int>()
        {
            {EtlItemType.Document, 0},
            {EtlItemType.Counter, 0}
        };

        public Dictionary<EtlItemType, int> NumberOfTransformedItems = new Dictionary<EtlItemType, int>()
        {
            {EtlItemType.Document, 0},
            {EtlItemType.Counter, 0}
        };

        public Dictionary<EtlItemType, long> LastExtractedEtags = new Dictionary<EtlItemType, long>
        {
            {EtlItemType.Document, 0},
            {EtlItemType.Counter, 0}
        };

        public Dictionary<EtlItemType, long> LastTransformedEtags = new Dictionary<EtlItemType, long>
        {
            {EtlItemType.Document, 0},
            {EtlItemType.Counter, 0}
        };
        
        public Dictionary<EtlItemType, long> LastFilteredOutEtags = new Dictionary<EtlItemType, long>
        {
            {EtlItemType.Document, 0},
            {EtlItemType.Counter, 0}
        };

        public long LastLoadedEtag;

        public int NumberOfLoadedItems;

        public int TransformationErrorCount;

        public string BatchCompleteReason;

        public string ChangeVector;

        public bool? SuccessfullyLoaded;
    }
}
