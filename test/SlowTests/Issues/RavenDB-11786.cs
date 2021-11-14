using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11786:RavenTestBase
    {
        public RavenDB_11786(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public async Task SubscriptionsWorksWithCounter(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {                    
                    User entity = new User
                    {
                        Name = "foobar"                        
                    };
                    session.Store(entity);                 
                    session.CountersFor(entity).Increment("Modifications");
                    session.SaveChanges();

                }
                var subsId = store.Subscriptions.Create<User>(new Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions<User>()
                {
                    Projection = x => new
                    {
                        Foo = RavenQuery.Counter(x, "Modifications"),
                        x.AddressId
                    }
                });

                var modificationsValue = 0;
                var subsWorker = store.Subscriptions.GetSubscriptionWorker(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
                {
                    CloseWhenNoDocsLeft = true
                });
                await Assert.ThrowsAsync<SubscriptionClosedException>(async ()=>
                await subsWorker.Run(x =>
                {
                    modificationsValue = Int32.Parse(x.Items[0].RawResult["Foo"].ToString());
                }));
                Assert.Equal(modificationsValue, 1);
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public async Task SubscriptionsWorksWithCompareExchange(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                string userId;
                string cmpxValueOriginal;
                using (var session = store.OpenSession())
                {                    
                    User user = new User
                    {
                        Name = "foobar"
                    };
                    session.Store(user);
                    userId = session.Advanced.GetDocumentId(user);
                    // Try to reserve a new user email 
                    // Note: This operation takes place outside of the session transaction, 
                    //       It is a cluster-wide reservation
                    cmpxValueOriginal = "His name is " + user.Name;
                    CompareExchangeResult<string> cmpXchgResult
                        = store.Operations.Send(
                            new PutCompareExchangeValueOperation<string>(user.Id, cmpxValueOriginal, 0));
                    session.SaveChanges();

                }
                var subsId = store.Subscriptions.Create<User>(new Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions<User>()
                {
                    Projection = x => new
                    {
                        Foo = RavenQuery.CmpXchg<string>(userId),
                        x.AddressId
                    }
                });

                string cmpxValue = string.Empty;
                var subsWorker = store.Subscriptions.GetSubscriptionWorker(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
                {
                    CloseWhenNoDocsLeft = true
                });
                await Assert.ThrowsAsync<SubscriptionClosedException>(async () =>
                await subsWorker.Run(x =>
                {
                    cmpxValue  = x.Items[0].RawResult["Foo"].ToString();
                }));
                Assert.Equal(cmpxValue, cmpxValueOriginal);
            }
        }
    }
}
