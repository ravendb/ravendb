using FastTests;
using Raven.Client;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

// The server reads the Quill application list straight out of the config database's documents, by document
// id prefix and property name, without ever referencing the App type. These assertions are the only thing
// holding the two ends together: rename or move something here and write-usage metering silently reports
// nothing - no compiler error, no failing endpoint test.
public class QuillMeteringContractTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Server_reads_the_property_the_app_document_actually_declares()
    {
        Assert.Equal(nameof(App.Database), Constants.Quill.ApplicationDatabasePropertyName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Server_scans_the_prefix_the_appliance_writes_apps_under()
    {
        Assert.Equal(AppLookup.IdPrefix, Constants.Quill.AppIdPrefix);
        Assert.Equal(Constants.Quill.AppIdPrefix + "my-app", AppLookup.DocumentIdFor("my-app"));
    }
}
