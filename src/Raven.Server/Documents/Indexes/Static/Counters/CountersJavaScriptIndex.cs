using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Indexes.Static.Counters;

public class CountersJavaScriptIndexJint : AbstractCountersAndTimeSeriesJavaScriptIndexJint
{
    public const string MapPrefix = "counters.";

    public CountersJavaScriptIndexJint(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
        : base(definition, configuration, MapPrefix, Constants.Counters.All, indexVersion)
    {
    }
}

public class CountersJavaScriptIndexV8 : AbstractCountersAndTimeSeriesJavaScriptIndexV8
{

    public CountersJavaScriptIndexV8(IndexDefinition definition, RavenConfiguration configuration, long indexVersion, CancellationToken cancellationToken)
        : base(definition, configuration, CountersJavaScriptIndexJint.MapPrefix, Constants.Counters.All, indexVersion, cancellationToken)
    {
    }
}
