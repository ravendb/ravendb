using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class CountersJavaScriptIndex : AbstractCountersAndTimeSeriesJavaScriptIndex
    {
        private const string MapPrefix = "counters.";

        public CountersJavaScriptIndex(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
            : base(definition, configuration, MapPrefix, Constants.Counters.All, indexVersion)
        {
        }
    }
}
