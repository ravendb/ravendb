using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15554 : RavenTestBase
    {
        public RavenDB_15554(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldCallEventListeners()
        {
            var listenerOneCalled = 0;
            var listenerTwoCalled = 0;

            using (var store = GetDocumentStore())
            {
                // Register listener 1
                store.OnAfterConversionToEntity += (sender, args) =>
                {
                    listenerOneCalled += 1;
                };

                // Insert data
                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Entity("bob", "bob"));
                    await session.SaveChangesAsync();
                }

                // FIRST LOAD
                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    // Register listener 2
                    session.Advanced.OnAfterConversionToEntity += (sender, args) =>
                    {
                        listenerTwoCalled += 1;
                    };

                    Entity entity = await session.LoadAsync<Entity>("bob");
                    Assert.Equal("bob", entity.Id);
                }

                // SECOND LOAD
                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    session.Advanced.OnAfterConversionToEntity += (sender, args) =>
                    {
                        listenerTwoCalled += 1;
                    };

                    Entity entity = await session.LoadAsync<Entity>("bob");
                    Assert.Equal("bob", entity.Id);
                }

                Assert.Equal(2, listenerOneCalled);
                Assert.Equal(2, listenerTwoCalled);
            }
        }

        private class Entity
        {
            public Entity(string id, string name)
            {
                Id = id;
                Name = name;
            }

            private Entity()
            {
            }

            public string Id { get; }

            public string Name { get; }
        }
    }
}
