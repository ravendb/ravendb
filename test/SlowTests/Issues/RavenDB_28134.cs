using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Subscriptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_28134(ITestOutputHelper output) : RavenTestBase(output)
{
    private class User
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [RavenData(false, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData(true, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenericCreateKeepsPinToMentorNodeAndArchivedDataProcessingBehavior(Options options, bool useAsync)
    {
        using var store = GetDocumentStore(options);

        var creationOptions = new SubscriptionCreationOptions<User>
        {
            MentorNode = "A",
            PinToMentorNode = true,
            ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
        };

        var name = useAsync
            ? await store.Subscriptions.CreateAsync(creationOptions)
            : store.Subscriptions.Create(creationOptions);

        var state = await store.Subscriptions.GetSubscriptionStateAsync(name);
        Assert.Equal("A", state.MentorNode);
        Assert.True(state.PinToMentorNode);
        Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, state.ArchivedDataProcessingBehavior);
    }
}
