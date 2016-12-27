using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NewClientTests;
using Raven.NewClient.Client.Document;

namespace NewClientTests.NewClient.Subscriptions
{
    public class SubscriptionTestBase: RavenTestBase
    {
        public class Thing
        {
            public string Name { get; set; }
        }

        protected async Task CreateDocuments(DocumentStore store, int amount)
        {
            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < amount; i++)
                {
                    await session.StoreAsync(new Thing
                    {
                        Name = $"ThingNo{i}"
                    });
                }
                await session.SaveChangesAsync();
            }
        }
    }
}
