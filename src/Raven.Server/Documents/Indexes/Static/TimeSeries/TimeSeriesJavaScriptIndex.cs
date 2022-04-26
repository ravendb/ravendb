using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries;

public sealed class TimeSeriesJavaScriptIndexJint : AbstractCountersAndTimeSeriesJavaScriptIndexJint
{
    public const string MapPrefix = "timeSeries.";

    public TimeSeriesJavaScriptIndexJint(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
        : base(definition, configuration, MapPrefix, Constants.TimeSeries.All, indexVersion)
    {
    }
}

public sealed class TimeSeriesJavaScriptIndexV8 : AbstractCountersAndTimeSeriesJavaScriptIndexV8
{
    public TimeSeriesJavaScriptIndexV8(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
        : base(definition, configuration, TimeSeriesJavaScriptIndexJint.MapPrefix, Constants.TimeSeries.All, indexVersion)
    {
    }
}
