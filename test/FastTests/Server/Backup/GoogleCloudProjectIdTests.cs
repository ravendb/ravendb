using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Backup;

public class GoogleCloudProjectIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData("""{ "Project_Id": "other", "project_id": "exact" }""", "exact")]
    [InlineData("""{ "project_id": "exact", "PROJECT_ID": "other" }""", "exact")]
    [InlineData("""{ "Project_Id": "first", "PROJECT_ID": "second" }""", "first")]
    [InlineData("""{ "type": "service_account", /* comment */ "project_id": "commented", }""", "commented")]
    [InlineData("""{ "project_id": 42 }""", "42")]
    [InlineData("""{ "project_id": null }""", null)]
    [InlineData("""{ "type": "service_account" }""", null)]
    public void ReadsProjectIdPreferringTheExactName(string json, string expected)
    {
        Assert.Equal(expected, RavenGoogleCloudClient.ReadProjectId(json));
    }
}
