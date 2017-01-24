using System.Threading.Tasks;
using Raven.NewClient.Client.Document;

namespace FastTests.Client.Subscriptions
{
    public abstract class SubscriptionTestBase : RavenNewTestBase
    {
        protected class Thing
        {
            public string Id { get; set; }
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
