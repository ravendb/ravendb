using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Tests.Infrastructure.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8450 : RavenTestBase
    {
        public RavenDB_8450(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData("hello\nthere", "\n", DatabaseMode = RavenDatabaseMode.All)]
        [RavenData("hello\r\nthere", "\r\n", DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetSubscriptionsResultsWithEscapeHandling(Options options, string input, string shouldNotContain)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new PersonWithAddress
                    {
                        Address = new Address
                        {
                            Country = input
                        }
                    });
                    s.SaveChanges();
                }

                var result = store.Operations.Send(new SubscriptionTryoutOperation(new SubscriptionTryout
                {
                    Query = "from PersonWithAddresses as u select { Self: u }"
                }));

                Assert.DoesNotContain(shouldNotContain, result);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void SubscriptionWithNoResultsShouldNotLoopWhenTesting(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var sw = Stopwatch.StartNew();
                store.Operations.Send(new SubscriptionTryoutOperation(new SubscriptionTryout
                {
                    Query = "from PersonWithAddresses where Name != 'John' AND Age > 20"
                }));

                Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10)); // default timeout is set to 15
            }
        }


    }
}
