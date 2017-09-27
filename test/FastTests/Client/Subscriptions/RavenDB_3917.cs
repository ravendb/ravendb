using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_3917 : RavenTestBase
    {
        [Fact]
        public async Task SmugglerShouldNotExportImportSubscribtionIdentities()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "store1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "store2"))
            {
                var subscriptionId = store1.Subscriptions.Create<User>(new SubscriptionCreationOptions<User>()
                {
                    Name = "Foo"
                });

                await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), store2.Smuggler);
                var subscription = store2.Subscriptions.Open(new SubscriptionConnectionOptions(subscriptionId));
                await Assert.ThrowsAsync<SubscriptionDoesNotExistException>(() => subscription.Run(x => { }));
            }
        }
    }
}
