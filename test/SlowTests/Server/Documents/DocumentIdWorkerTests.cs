using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Server;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents;

public class DocumentIdWorkerTests : ClusterTestBase
{
    public DocumentIdWorkerTests(ITestOutputHelper output) : base(output)
    {
    }

    private class TestObj
    {
        public string Id { get; set; }
        public string Prop { get; set; }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IdWithEscapedAndControlCharacters_WhenReplicate_CanGet(bool withNonAscii)
    {
        var id = "A" + (char)1 + '\n';
        if (withNonAscii)
            id += 'Ć';
        const int replicas = 2;

        var (nodes, leader) = await CreateRaftCluster(replicas, watcherCluster: true);

        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = replicas, });
        using var s1 = GetDocumentStoreForNode(store, nodes[0]);
        using var s2 = GetDocumentStoreForNode(store, nodes[1]);

        using (var session = s1.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = id }, id);
            await session.SaveChangesAsync();
        }

        var loaded = await AssertWaitForNotNullAsync(async () =>
        {
            using var session = s2.OpenAsyncSession();
            return await session.Query<TestObj>().SingleOrDefaultAsync();
        });

        using (var session = s2.OpenAsyncSession())
        {
            var load = await session.LoadAsync<TestObj>(loaded.Id);
            Assert.NotNull(load);
        }

        using (var session = s1.OpenAsyncSession())
        {
            var load = await session.LoadAsync<TestObj>(id);
            Assert.NotNull(load);
        }
    }

    [RavenTheory(RavenTestCategory.Core, Skip = "RavenDB-24940")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IdWithControlCharacters_WhenReplicate_CanGetAndDeleteByNotEscapedId(bool withNonAscii)
    {
        var id = "A" + (char)1;
        if (withNonAscii)
            id += 'Ć';
        const int replicas = 2;

        var (nodes, leader) = await CreateRaftCluster(replicas, watcherCluster: true);

        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = replicas, });
        using var s1 = GetDocumentStoreForNode(store, nodes[0]);
        using var s2 = GetDocumentStoreForNode(store, nodes[1]);

        using (var session = s1.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = id }, id);
            await session.SaveChangesAsync();
        }

        await AssertWaitForTrueAsync(async () =>
        {
            using var session = s2.OpenAsyncSession();
            return await session.Query<TestObj>().CountAsync() == 1;
        });

        using (var session = s2.OpenAsyncSession())
        {
            var load = await session.LoadAsync<TestObj>(id);
            Assert.NotNull(load);
            session.Delete(id);
            await session.SaveChangesAsync();
        }

        using (var session = s2.OpenAsyncSession())
        {
            var count = await session.Query<TestObj>().CountAsync();
            Assert.Equal(0, count);
        }

        await AssertWaitForTrueAsync(async () =>
        {
            using var session = s1.OpenAsyncSession();
            return await session.Query<TestObj>().CountAsync() == 0;
        });
    }

    [RavenTheory(RavenTestCategory.Core, Skip = "RavenDB-24940")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IdWithControlCharacters_WhenReplicate_CanGetAndDeleteByEscapedId(bool withNonAscii)
    {
        var id = "A" + (char)1;
        if (withNonAscii)
            id += 'Ć';
        const int replicas = 2;

        var (nodes, leader) = await CreateRaftCluster(replicas, watcherCluster: true);

        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = replicas, });
        using var s1 = GetDocumentStoreForNode(store, nodes[0]);
        using var s2 = GetDocumentStoreForNode(store, nodes[1]);

        using (var session = s1.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = id }, id);
            await session.SaveChangesAsync();
        }

        await AssertWaitForTrueAsync(async () =>
        {
            using var session = s1.OpenAsyncSession();
            return await session.Query<TestObj>().CountAsync() == 1;
        });

        using (var session2 = s2.OpenAsyncSession())
        {
            var loadFrom2 = await session2.Query<TestObj>().SingleAsync();
            using var session1 = s1.OpenAsyncSession();
            var loadFrom1 = await session1.LoadAsync<TestObj>(loadFrom2.Id);
            Assert.NotNull(loadFrom1);
            session1.Delete(loadFrom2.Id);
            await session1.SaveChangesAsync();
        }

        using (var session = s1.OpenAsyncSession())
        {
            var count = await session.Query<TestObj>().CountAsync();
            Assert.Equal(0, count);
        }

        await AssertWaitForTrueAsync(async () =>
        {
            using var session = s2.OpenAsyncSession();
            return await session.Query<TestObj>().CountAsync() == 0;
        });
    }

    [RavenTheory(RavenTestCategory.Core, Skip = "RavenDB-24940")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IdWithControlCharacters_WhenReplicateModification_CanGetAndDeleteByNotEscapedId(bool withNonAscii)
    {
        var id = "A" + (char)1;
        if (withNonAscii)
            id += 'Ć';
        const int replicas = 3;

        var (nodes, leader) = await CreateRaftCluster(replicas, watcherCluster: true);

        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = replicas, });
        using var s1 = GetDocumentStoreForNode(store, nodes[0]);
        using var s2 = GetDocumentStoreForNode(store, nodes[1]);

        using (var session = s2.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj { Prop = id }, id);
            await session.SaveChangesAsync();
        }

        await AssertWaitForTrueAsync(async () =>
        {
            using var session = s2.OpenAsyncSession();
            return await session.Query<TestObj>().CountAsync() == 1;
        });

        using (var session2 = s2.OpenAsyncSession())
        {
            var load = await session2.LoadAsync<TestObj>(id);
            load.Prop += "changed";
            await session2.SaveChangesAsync();
        }

        await AssertWaitForTrueAsync(async () =>
        {
            using var session = s1.OpenAsyncSession();
            var load = await session.Query<TestObj>().SingleAsync();
            return load.Prop.EndsWith("changed");
        });

        WaitForUserToContinueTheTest(store);
        using (var session = s1.OpenAsyncSession())
        {
            var load = await session.LoadAsync<TestObj>(id);
            Assert.NotNull(load);
            session.Delete(id);
            await session.SaveChangesAsync();
        }

        using (var session = s1.OpenAsyncSession())
        {
            var count = await session.Query<TestObj>().CountAsync();
            Assert.Equal(0, count);
        }
    }


    [RavenTheory(RavenTestCategory.Core, Skip = "RavenDB-24940")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IdWithControlCharacters_WhenGetDocumentIdFromReplicatedDocument_ShouldBeEqualToTheOriginId(bool withNonAscii)
    {
        var id = "A" + (char)1 + '\n';
        if (withNonAscii)
            id += 'Ć';
        const int replicas = 2;

        var (nodes, leader) = await CreateRaftCluster(replicas, watcherCluster: true);

        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = replicas, });

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = id }, id);
            await session.SaveChangesAsync();
        }

        await Task.WhenAll(nodes.Select(async server =>
        {
            using var s = new DocumentStore
            {
                Database = store.Database,
                Urls = [server.WebUrl],
            };
            s.Conventions.DisableTopologyUpdates = true;
            s.Initialize();

            await AssertWaitForTrueAsync(async () =>
            {
                using var session = s.OpenAsyncSession();
                return await session.Query<User>().AnyAsync();
            });

            using var session = s.OpenAsyncSession();
            var user = await session.Query<User>().SingleAsync();
            Assert.Equal(id, user.Id);
        }));
    }

    private static IDocumentStore GetDocumentStoreForNode(DocumentStore store, RavenServer server)
    {
        return new DocumentStore
        {
            Database = store.Database,
            Urls = [server.WebUrl],
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize();
    }
}
