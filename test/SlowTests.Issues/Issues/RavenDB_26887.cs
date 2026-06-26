using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26887 : RavenTestBase
{
    public RavenDB_26887(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task Indexes_open_unshared_instead_of_faulting_when_they_cannot_hard_link_to_SharedJournals()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = false });

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        await database.IndexStore.InitializeSharedJournalsAsync();
        Assert.NotNull(database.IndexStore.SharedJournals);

        // Simulate the index journals not being hard-linkable to @SharedJournals - i.e. @SharedJournals sits on a
        // different volume than the indexes (errno 17, "cannot move the file to a different disk drive"). The gate
        // must probe @SharedJournals (not the database journals) and, finding it can't link, open each index in
        // unshared mode instead of faulting it on the failed cross-device LinkFiles call.
        database.IndexStore.SharedJournals.Env.Options.ForTestingPurposesOnly().SimulateCannotLinkJournals = true;

        for (int i = 0; i < 3; i++)
        {
            await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
            {
                Name = $"Users/ByName_{i}",
                Maps = { "from u in docs.Users select new { u.Name }" }
            }));
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Joe" });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // The bug faulted every index here with IndexOpenException -> FaultyInMemoryIndex.
        var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        var faulty = stats.Indexes.Where(i => i.State == IndexState.Error || i.Type == IndexType.Faulty).ToList();
        Assert.True(faulty.Count == 0, $"faulty indexes: {string.Join(", ", faulty.Select(f => $"{f.Name}({f.State}/{f.Type})"))}");

        // Every index must run unshared (shared journals correctly disabled because it can't link to @SharedJournals).
        // If the gate regresses to probing the database journals, these stay co-located on one volume, the probe
        // passes, and the indexes come up as branches - so RootJournal would be non-null here.
        var indexes = database.IndexStore.GetIndexes().ToList();
        Assert.Equal(3, indexes.Count);
        foreach (var idx in indexes)
            Assert.Null(idx._environment.Options.RootJournal);

        // Data is fully indexed and queryable.
        foreach (var idx in indexes)
        {
            using var session = store.OpenAsyncSession();
            var results = await session.Advanced.AsyncRawQuery<User>($"from index '{idx.Name}' where Name = 'Joe'").ToListAsync();
            Assert.Equal(1, results.Count);
        }
    }
}
