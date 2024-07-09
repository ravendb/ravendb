using System.ComponentModel;

namespace Raven.Server.Documents.Indexes
{
    public enum IndexItemType
    {
        None,

        [Description("Document Item")]
        Document,

        [Description("Time Series Item")]
        TimeSeries,

        [Description("Time Series Deleted Range Item")]
        TimeSeriesDeletedRange,

        [Description("Counter Item")]
        Counters
    }
}
