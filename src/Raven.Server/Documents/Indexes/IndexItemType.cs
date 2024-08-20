using System.ComponentModel;

namespace Raven.Server.Documents.Indexes
{
    public enum IndexItemType
    {
        None,

        [Description("document")]
        Document,

        [Description("time series item")]
        TimeSeries,

        [Description("time series deleted range")]
        TimeSeriesDeletedRange,

        [Description("counter")]
        Counters
    }
}
