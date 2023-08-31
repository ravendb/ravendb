using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17154 : RavenTestBase
    {
        public RavenDB_17154(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task InvokeOnAfterConversionToEntityAfterTrackingEntityInSession()
        {
            var listenerOneCalled = 0;
            var listenerOneDocExists = 0;
            var listenerTwoCalled = 0;
            var listenerTwoDocExists = 0;

            using (var store = GetDocumentStore())
            {
                // Register listener 1
                store.OnAfterConversionToEntity += (sender, args) =>
                {
                    Interlocked.Increment(ref listenerOneCalled);
                    var metadata = args.Session.GetMetadataFor((Entity)(args.Entity));
                    if (metadata != null)
                        Interlocked.Increment(ref listenerOneDocExists);
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
                        Interlocked.Increment(ref listenerTwoCalled);
                        var metadata = args.Session.GetMetadataFor((Entity)(args.Entity));
                        if (metadata != null)
                            Interlocked.Increment(ref listenerTwoDocExists);
                    };

                    Entity entity = await session.LoadAsync<Entity>("bob");
                    Assert.Equal("bob", entity.Id);
                }

                // SECOND LOAD
                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    // Register listener 2
                    session.Advanced.OnAfterConversionToEntity += (sender, args) =>
                    {
                        Interlocked.Increment(ref listenerTwoCalled);
                        var metadata = args.Session.GetMetadataFor((Entity)(args.Entity));
                        if (metadata != null)
                            Interlocked.Increment(ref listenerTwoDocExists);
                    };

                    Entity entity = await session.LoadAsync<Entity>("bob");
                    Assert.Equal("bob", entity.Id);
                }

                Assert.Equal(2, listenerOneCalled);
                Assert.Equal(2, listenerTwoCalled);
                Assert.Equal(2, listenerOneDocExists);
                Assert.Equal(2, listenerTwoDocExists);
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
