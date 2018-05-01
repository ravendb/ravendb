using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents.Operations.Counters;
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

                store.Operations.Send(new IncrementCounterOperation("users/1-A", "likes", 10));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out object counters));
                    Assert.Equal(1, ((object[])counters).Length);
                    Assert.True(((object[])counters).Contains("likes"));
                }

                store.Operations.Send(new IncrementCounterOperation("users/1-A", "votes", 50));
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out object counters));
                    Assert.Equal(2, ((object[])counters).Length);
                    Assert.True(((object[])counters).Contains("likes"));
                    Assert.True(((object[])counters).Contains("votes"));
                }

                store.Operations.Send(new DeleteCounterOperation("users/1-A", "likes"));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out object counters));
                    Assert.Equal(1, ((object[])counters).Length);
                    Assert.True(((object[])counters).Contains("votes"));
                }

                store.Operations.Send(new DeleteCounterOperation("users/1-A", "votes"));
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);
                    Assert.False(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out _));
                }

            }
        }
    }
}
