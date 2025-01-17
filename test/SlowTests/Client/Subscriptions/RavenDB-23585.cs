using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions;

public class RavenDB_23585(ITestOutputHelper output) : SubscriptionTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector)]
    public async Task SubscriptionWillThrowExceptionWhenUsingVectorInQuery()
    {
        using (var store = GetDocumentStore())
        {
            var subscriptionCreationParams = new SubscriptionCreationOptions
            {
                Query = "from People where vector.search(Field, 'hello')"
            };
            
            var exception = await Assert.ThrowsAsync<RavenException>(() => store.Subscriptions.CreateAsync(subscriptionCreationParams));
            Assert.Contains("'vector.search' query method is not supported for this type of query", exception.Message);
        }
    }
}
