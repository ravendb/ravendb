using System.Globalization;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Basic;

public class ConfigurationCultureTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Configuration)]
    public void FractionalSettingIsParsedWithTheInvariantCulture()
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("nl-NL");

            RavenConfiguration configuration = RavenConfiguration.CreateForTesting("db", ResourceType.Database);
            configuration.SetSetting("Indexing.Corax.VectorSearch.DefaultMinimumSimilarity", "0.25");
            configuration.Initialize();

            Assert.Equal(0.25f, configuration.Indexing.CoraxVectorSearchDefaultMinimumSimilarity);
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }
}
