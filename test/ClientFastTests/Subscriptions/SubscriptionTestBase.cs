using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using NewClientTests;
using Raven.NewClient.Client.Document;
using Tests.Infrastructure;

namespace NewClientTests.NewClient.Subscriptions
{
    public class SubscriptionTestBase: RavenNewTestBase
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
