using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Graph;
using FastTests.Server.Replication;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;
public class RavenDB_21101 : ReplicationTestBase
{
    public RavenDB_21101(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task RevisionsConflictConfiguration_Default_Value_Is_1024()
    {
        using var src = GetDocumentStore();
        using var dst = GetDocumentStore();

        var srcDb = await GetDatabase(src.Database);
        var dstDb = await GetDatabase(dst.Database);

        AssertDefaultConflictConfiguration(srcDb);
        AssertDefaultConflictConfiguration(dstDb);

        await SetupReplicationAsync(src, dst); // Conflicts resolved
        
        // creating 1050 conflict revisions (which are going to shrink to 1024 and creating 1 notification for exceeding to the max revisions count that allowed)
        var id = "Docs/1";
        for (int i = 0; i < 350; i++)
        {
            using (var session = dst.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = $"Dst{i}" }, id);
                await session.SaveChangesAsync();
            }
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = $"Src{i}" }, id);
                await session.SaveChangesAsync();
            }
            await EnsureReplicatingAsync(src, dst);
        }
        
        using (var session = dst.OpenAsyncSession())
        {
            var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync(id);
            Assert.Equal(1024, doc1RevCount);
        }
    }

    private void AssertDefaultConflictConfiguration(DocumentDatabase database)
    {
        var conflictConfig = database.DocumentsStorage.RevisionsStorage.ConflictConfiguration;
        Assert.NotNull(conflictConfig);
        Assert.Null(conflictConfig.Default.MinimumRevisionAgeToKeep);
        Assert.Equal(conflictConfig.Default.MinimumRevisionsToKeep, 1024);
    }

}

