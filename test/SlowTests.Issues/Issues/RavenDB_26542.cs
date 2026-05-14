using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_26542(ITestOutputHelper output) : RavenTestBase(output)
    {
        public record ProjectionResult(long? Counter, string AddressId);
        
        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Counters)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task SubscriptionsWorksWithRecordType(Options options)
        {
            const string counter = "Modifications";
            
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {                    
                    User entity = new()
                    {
                        Name = "foobar"                        
                    };
                    session.Store(entity);                 
                    session.CountersFor(entity).Increment(counter);
                    session.SaveChanges();
                }

                var subsId = await store.Subscriptions.CreateAsync(new Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions<User>
                {
                    Projection = x => new ProjectionResult(RavenQuery.Counter(x, counter), x.AddressId)
                });

                long? modificationsValue = 0;
                var subsWorker = store.Subscriptions.GetSubscriptionWorker<ProjectionResult>(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
                {
                    CloseWhenNoDocsLeft = true
                });
                await Assert.ThrowsAsync<SubscriptionClosedException>(async ()=>
                await subsWorker.Run(x =>
                {
                    modificationsValue = x.Items[0].Result.Counter;
                }));
                Assert.Equal(1L, modificationsValue);
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task SubscriptionProjectionWithCtorDoesNotWrapInParentheses()
        {
            const string counter = "Modifications";

            using (var store = GetDocumentStore())
            {
                string subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Projection = x => new ProjectionResult(RavenQuery.Counter(x, counter), x.AddressId)
                });

                System.Collections.Generic.List<SubscriptionState> subs = await store.Subscriptions.GetSubscriptionsAsync(0, 10);
                string query = subs.First(s => s.SubscriptionName == subsId).Query;

                Assert.Contains("select {", query);
                Assert.DoesNotContain("select ({", query);
            }
        }
    }
}
