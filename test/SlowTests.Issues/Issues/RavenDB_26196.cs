using FastTests;
using Raven.Client.Documents.Operations.ETL.Queue;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26196(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Etl)]
    public void InvalidHttpConnectionStringWithoutQueueEndpointFailsValidation()
    {
        var settings = new AzureQueueStorageConnectionSettings
        {
            ConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        };
        Assert.False(settings.IsValidConnection());
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void MalformedConnectionStringFailsValidation()
    {
        var settings = new AzureQueueStorageConnectionSettings
        {
            ConnectionString = "not-a-valid-connection-string"
        };
        Assert.False(settings.IsValidConnection());
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void ValidHttpConnectionStringPassesValidation()
    {
        var settings = new AzureQueueStorageConnectionSettings
        {
            ConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
        };
        Assert.True(settings.IsValidConnection());
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void ValidHttpsConnectionStringPassesValidation()
    {
        var settings = new AzureQueueStorageConnectionSettings
        {
            ConnectionString = "DefaultEndpointsProtocol=https;AccountName=myexamplestorage;AccountKey=myaccountkey;EndpointSuffix=core.windows.net"
        };
        Assert.True(settings.IsValidConnection());
    }
}
