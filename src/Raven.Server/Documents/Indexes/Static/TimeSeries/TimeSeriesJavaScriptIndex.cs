using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public sealed class TimeSeriesJavaScriptIndex : AbstractCountersAndTimeSeriesJavaScriptIndex
    {
        private const string MapPrefix = "timeSeries.";

        public TimeSeriesJavaScriptIndex(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
            : base(definition, configuration, MapPrefix, Constants.TimeSeries.All, indexVersion)
        {
        }
    }
}
