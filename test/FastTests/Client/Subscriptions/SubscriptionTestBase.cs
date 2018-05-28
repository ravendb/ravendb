using System.Threading.Tasks;
using Raven.Client.Documents;

namespace FastTests.Client.Subscriptions
{
    public abstract class SubscriptionTestBase : RavenTestBase
    {
        protected class Thing
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public Thing OtherDoc { get; set; }

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
