using System.Linq;
using Raven.Server.Config;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_5042 : NoDisposalNeeded
    {
        public RavenDB_5042(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanGenerateConfigurationEntryMetadata()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configurationEntries = RavenConfiguration.AllConfigurationEntries.Value;

                var djv = new DynamicJsonValue
                {
                    ["Values"] = new DynamicJsonArray(configurationEntries.Select(x => x.ToJson()))
                };

                _ = context.ReadObject(djv, "configuration/entries");
            }
        }
    }
}
