using System.IO;
using Raven.Client.Documents.Operations.AI;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class AiVertexSettingsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData("{\"type\":\"service_account\",\"project_id\":\"my-project\"}")]
    [InlineData("{\"type\":\"service_account\",\"Project_Id\":\"my-project\"}")]
    [InlineData("{\"nested\":{\"project_id\":\"other\"},\"project_id\":\"my-project\"}")]
    public void GetProjectIdReadsProjectIdIgnoringCase(string credentialsJson)
    {
        var settings = new VertexSettings("model", credentialsJson, "us-central1");

        Assert.Equal("my-project", settings.GetProjectId());
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void GetProjectIdReturnsNullForNullProjectId()
    {
        var settings = new VertexSettings("model", "{\"project_id\":null}", "us-central1");

        Assert.Null(settings.GetProjectId());
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void GetProjectIdThrowsWhenProjectIdIsMissing()
    {
        var settings = new VertexSettings("model", "{\"type\":\"service_account\"}", "us-central1");

        Assert.Throws<InvalidDataException>(() => settings.GetProjectId());
    }
}
