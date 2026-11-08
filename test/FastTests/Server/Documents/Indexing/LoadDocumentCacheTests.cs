using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing;

/// <summary>
/// LoadDocument keeps what it read for a key until the read transaction is renewed.
/// </summary>
[Collection(LoadDocumentCacheTestsCollection.Name)]
public class LoadDocumentCacheTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private sealed class Owner
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private sealed class Company
    {
        public string Id { get; set; }
        public string OwnerId { get; set; }
    }

    private sealed class Result
    {
        public string OwnerName { get; set; }
        public string Cv { get; set; }
        public string State { get; set; }
    }

    private const string IndexName = "Companies/ByOwner";

    private static IndexDefinition OwnerIndex(string map, params string[] exactFields)
    {
        var definition = new IndexDefinition { Name = IndexName, Maps = { map } };
        foreach (var field in exactFields)
            definition.Fields[field] = new IndexFieldOptions { Indexing = FieldIndexing.Exact };

        return definition;
    }

    private void Seed(IDocumentStore store, int owners, int companiesPerOwner)
    {
        using var bulk = store.BulkInsert();
        for (var o = 0; o < owners; o++)
            bulk.Store(new Owner { Id = $"owners/{o}", Name = $"Owner {o}" });

        for (var c = 0; c < owners * companiesPerOwner; c++)
            bulk.Store(new Company { Id = $"companies/{c}", OwnerId = $"owners/{c % owners}" });
    }

    private static int CountByOwnerName(IDocumentStore store, string name)
    {
        using var session = store.OpenSession();
        return session.Query<Result>(IndexName).Count(x => x.OwnerName == name);
    }

    private static int CountWithoutOwner(IDocumentStore store)
    {
        using var session = store.OpenSession();
        return session.Query<Result>(IndexName).Count(x => x.State == "no owner");
    }

    private const string MapWithState =
        "from c in docs.Companies let o = LoadDocument(c.OwnerId, \"Owners\") select new { OwnerName = o.Name, State = o == null ? \"no owner\" : \"owner\" }";

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void EveryDocumentThatLoadsTheSameDocumentSeesIt(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(OwnerIndex(
            "from c in docs.Companies let o = LoadDocument(c.OwnerId, \"Owners\") select new { OwnerName = o.Name }", "OwnerName")));

        Seed(store, owners: 5, companiesPerOwner: 400); // 400 documents load every owner in the same batch
        Indexes.WaitForIndexing(store);

        for (var o = 0; o < 5; o++)
            Assert.Equal(400, CountByOwnerName(store, $"Owner {o}"));
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AChangeOfTheLoadedDocumentIsSeenByTheNextBatch(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(OwnerIndex(MapWithState, "OwnerName", "State")));

        Seed(store, owners: 3, companiesPerOwner: 100);
        Indexes.WaitForIndexing(store);
        Assert.Equal(100, CountByOwnerName(store, "Owner 1"));

        using (var session = store.OpenSession())
        {
            session.Load<Owner>("owners/1").Name = "Renamed";
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        Assert.Equal(100, CountByOwnerName(store, "Renamed"));
        Assert.Equal(0, CountByOwnerName(store, "Owner 1"));

        // deleted: the cache of the previous batch must not bring it back
        using (var session = store.OpenSession())
        {
            session.Delete("owners/1");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        Assert.Equal(0, CountByOwnerName(store, "Renamed"));
        Assert.Equal(100, CountWithoutOwner(store));

        // ... and created again
        using (var session = store.OpenSession())
        {
            session.Store(new Owner { Id = "owners/1", Name = "Back" });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        Assert.Equal(100, CountByOwnerName(store, "Back"));
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ADocumentThatDidNotExistWhenItWasLoadedIsPickedUpWhenItIsCreated(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(OwnerIndex(MapWithState, "OwnerName", "State")));

        using (var bulk = store.BulkInsert())
        {
            for (var c = 0; c < 200; c++)
                bulk.Store(new Company { Id = $"companies/{c}", OwnerId = "owners/late" });
        }

        Indexes.WaitForIndexing(store);
        Assert.Equal(200, CountWithoutOwner(store));

        using (var session = store.OpenSession())
        {
            session.Store(new Owner { Id = "owners/late", Name = "Late" });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        Assert.Equal(200, CountByOwnerName(store, "Late"));
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void RenewingTheReadTransactionInTheMiddleOfABatchKeepsTheResultsRight(Options options)
    {
        // renew the read transaction after every document
        options.ModifyDatabaseRecord += record => record.Settings["Indexing.MaxTimeForDocumentTransactionToRemainOpenInSec"] = "0";

        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(OwnerIndex(
            "from c in docs.Companies let o = LoadDocument(c.OwnerId, \"Owners\") select new { OwnerName = o.Name }", "OwnerName")));

        Seed(store, owners: 3, companiesPerOwner: 300);
        Indexes.WaitForIndexing(store);

        for (var o = 0; o < 3; o++)
            Assert.Equal(300, CountByOwnerName(store, $"Owner {o}"));

        // a change while the documents are mapped again
        using (var session = store.OpenSession())
        {
            session.Load<Owner>("owners/2").Name = "Changed";
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        Assert.Equal(300, CountByOwnerName(store, "Changed"));
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void TheCollectionIsCheckedForEveryCallOfTheSameDocument(Options options)
    {
        using var store = GetDocumentStore(options);

        // loaded twice, once with its collection and once with another
        store.Maintenance.Send(new PutIndexesOperation(OwnerIndex(
            "from c in docs.Companies " +
            "let right = LoadDocument(c.OwnerId, \"Owners\") " +
            "let wrong = LoadDocument(c.OwnerId, \"Departments\") " +
            "select new { OwnerName = right.Name, State = wrong == null ? \"none\" : \"some\" }", "OwnerName", "State")));

        Seed(store, owners: 2, companiesPerOwner: 50);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        Assert.Equal(100, session.Query<Result>(IndexName).Count(x => x.State == "none"));
        Assert.Equal(0, session.Query<Result>(IndexName).Count(x => x.State == "some"));
        Assert.Equal(50, session.Query<Result>(IndexName).Count(x => x.OwnerName == "Owner 0"));
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void TheChangeVectorOfALoadedDocumentIsAvailableToTheMap(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutIndexesOperation(OwnerIndex(
            "from c in docs.Companies let o = LoadDocument(c.OwnerId, \"Owners\") select new { Cv = o[\"@change-vector\"] }", "Cv")));

        Seed(store, owners: 2, companiesPerOwner: 20);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var owner = session.Load<Owner>("owners/0");
        var changeVector = session.Advanced.GetChangeVectorFor(owner);
        Assert.False(string.IsNullOrEmpty(changeVector));
        Assert.Equal(20, session.Query<Result>(IndexName).Count(x => x.Cv == changeVector));
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void TheDocumentsKeptForABatchAreBoundedAndTheResultsDoNotDependOnIt(Options options)
    {
        // every key is loaded twice, far apart, with the bound reached
        const int owners = 300;
        const int cap = 50;

        var previousCap = CurrentIndexingScope.MaxKeptLoadedDocuments;
        var previousHighest = CurrentIndexingScope.HighestNumberOfKeptLoadedDocuments;
        CurrentIndexingScope.MaxKeptLoadedDocuments = cap;
        CurrentIndexingScope.HighestNumberOfKeptLoadedDocuments = 0;
        try
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutIndexesOperation(OwnerIndex(MapWithState, "OwnerName", "State")));

            Seed(store, owners, companiesPerOwner: 2);
            Indexes.WaitForIndexing(store);

            Assert.True(CurrentIndexingScope.HighestNumberOfKeptLoadedDocuments <= cap,
                $"{CurrentIndexingScope.HighestNumberOfKeptLoadedDocuments} loaded documents were kept for a batch, the bound is {cap}");

            Assert.Equal(2, CountByOwnerName(store, "Owner 0"));
            Assert.Equal(2, CountByOwnerName(store, $"Owner {owners - 1}"));
            Assert.Equal(0, CountWithoutOwner(store));
        }
        finally
        {
            CurrentIndexingScope.MaxKeptLoadedDocuments = previousCap;
            CurrentIndexingScope.HighestNumberOfKeptLoadedDocuments = previousHighest;
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void ManyDocumentsLoadingTheSameDocumentDoNotAllocateForEveryCall()
    {
        const int owners = 25;
        const int companies = 5_000;

        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var bulk = store.BulkInsert())
        {
            for (var o = 0; o < owners; o++)
                bulk.Store(new Owner { Id = $"owners/{o}", Name = $"Owner {o}" });

            for (var c = 0; c < companies; c++)
                bulk.Store(new Company { Id = $"companies/{c}", OwnerId = $"owners/{c % owners}" });
        }

        // the first index pays for compilation and JIT
        BuildIndex(store, "Companies/Warmup", "from c in docs.Companies select new { c.OwnerId }");

        // the counter is process-wide, take the smallest of a few builds
        const string withoutLoadsMap = "from c in docs.Companies select new { c.OwnerId, OwnerName = c.OwnerId }";
        const string withLoadsMap = "from c in docs.Companies let o = LoadDocument(c.OwnerId, \"Owners\") select new { c.OwnerId, OwnerName = o.Name }";
        var withoutLoads = Enumerable.Range(0, 2).Min(i => BuildIndex(store, $"Companies/WithoutLoads{i}", withoutLoadsMap));
        var withLoads = Enumerable.Range(0, 4).Min(i => BuildIndex(store, $"Companies/WithLoads{i}", withLoadsMap));

        var perCall = (withLoads - withoutLoads) / (double)companies;
        output.WriteLine($"managed allocation of the build: {withoutLoads / 1024.0 / 1024:N1} MB without the loads, {withLoads / 1024.0 / 1024:N1} MB with them = {perCall:F0} bytes per LoadDocument call");

        // a call answered from the batch cache does not read the document again
        Assert.True(perCall < 4_500, $"A LoadDocument call allocated {perCall:F0} bytes");
    }

    private static long BuildIndex(IDocumentStore store, string name, string map)
    {
        var before = GC.GetTotalAllocatedBytes(precise: true);
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Name = name, Maps = { map } }));
        WaitForIndexingOf(store, name);
        return GC.GetTotalAllocatedBytes(precise: true) - before;
    }

    private static void WaitForIndexingOf(IDocumentStore store, string name)
    {
        var timeout = DateTime.UtcNow.AddMinutes(2);
        while (DateTime.UtcNow < timeout)
        {
            var stats = store.Maintenance.Send(new GetIndexStatisticsOperation(name));
            if (stats.IsStale == false)
                return;

            System.Threading.Thread.Sleep(20);
        }

        throw new TimeoutException($"Index {name} did not become non-stale");
    }
}

// the allocation test counts all threads, so no other test may run meanwhile
[CollectionDefinition(Name, DisableParallelization = true)]
public class LoadDocumentCacheTestsCollection
{
    public const string Name = nameof(LoadDocumentCacheTestsCollection);
}
