using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlRunStats
    {
        public int NumberOfExtractedItems;

        public int NumberOfTransformedItems;

        public Dictionary<EtlItemType, long> LastTransformedEtag = new Dictionary<EtlItemType, long>()
        {
            {EtlItemType.Document, 0},
            {EtlItemType.Counter, 0}
        };

        public long LastLoadedEtag;

        public long LastFilteredOutEtag;

        public string BatchCompleteReason;

        public string ChangeVector;
    }
}
