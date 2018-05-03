using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class CountersInMetadata : ReplicationTestBase
    {
        [Fact]
        public void IncrementAndDeleteShouldChangeDocumentMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.SaveChanges();
                }

                store.Counters.Increment("users/1-A", "likes", 10);

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out object counters));
                    Assert.Equal(1, ((object[])counters).Length);
                    Assert.True(((object[])counters).Contains("likes"));
                }

                store.Counters.Increment("users/1-A", "votes", 50);
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out object counters));
                    Assert.Equal(2, ((object[])counters).Length);
                    Assert.True(((object[])counters).Contains("likes"));
                    Assert.True(((object[])counters).Contains("votes"));
                }

                store.Counters.Delete("users/1-A", "likes");

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out object counters));
                    Assert.Equal(1, ((object[])counters).Length);
                    Assert.True(((object[])counters).Contains("votes"));
                }

                store.Counters.Delete("users/1-A", "votes");
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);
                    Assert.False(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out _));
                }

            }
        }

        [Fact]
        public async Task ConflictsInMetadata()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1-A");
                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);

                await storeB.Counters.IncrementAsync("users/1-A", "likes", 14);
                await storeB.Counters.IncrementAsync("users/1-A", "dislikes", 13);

                await storeA.Counters.IncrementAsync("users/1-A", "likes", 12);
                await storeA.Counters.IncrementAsync("users/1-A", "cats", 11);

                EnsureReplicating(storeA, storeB);

                var val = await storeB.Counters.GetAsync("users/1-A", "likes");
                Assert.Equal(26, val);

                val = await storeB.Counters.GetAsync("users/1-A", "dislikes");
                Assert.Equal(13, val);

                val = await storeB.Counters.GetAsync("users/1-A", "cats");
                Assert.Equal(11, val);

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1-A");
                    var counters = (object[])session.Advanced.GetMetadataFor(user)["@counters"];

                    Assert.Equal(3, counters.Length);
                    // verify that counters are sorted
                    Assert.Equal("cats", counters[0]);
                    Assert.Equal("dislikes", counters[1]);
                    Assert.Equal("likes", counters[2]);

                }
            }
        }
    }
}
