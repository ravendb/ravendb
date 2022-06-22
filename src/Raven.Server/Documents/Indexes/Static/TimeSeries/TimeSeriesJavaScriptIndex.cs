using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Patch.Jint;

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
    public TimeSeriesJavaScriptIndexV8(IndexDefinition definition, RavenConfiguration configuration, long indexVersion, CancellationToken token)
        : base(definition, configuration, TimeSeriesJavaScriptIndexJint.MapPrefix, Constants.TimeSeries.All, indexVersion, token)
    {
    }
}
