using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Queries.Timings;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27565 : RavenTestBase
{
    public RavenDB_27565(ITestOutputHelper output) : base(output)
    {
    }

    private class Movie
    {
        public string Id { get; set; }

        public long First { get; set; }

        public object[] Rest { get; set; }
    }

    // Tag is declared, so the order by carries a real field id, and the extra terms arrive through CreateField,
    // which used to leave the multiple-terms marker unset
    private class Movies_ByTag : AbstractIndexCreationTask<Movie>
    {
        public Movies_ByTag()
        {
            Map = movies => from m in movies
                            select new
                            {
                                Tag = m.First,
                                _ = m.Rest.Select(t => CreateField("Tag", t))
                            };
        }
    }

    private class Item
    {
        public string Id { get; set; }

        public string[] Tags { get; set; }
    }

    private class Items_ByTag : AbstractIndexCreationTask<Item>
    {
        public Items_ByTag()
        {
            Map = items => from i in items
                           select new
                           {
                               _ = i.Tags.Select(t => CreateField("Tag", t))
                           };
        }
    }

    // the term cap counts terms, so a document owning five of them must not cost the page its second document
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CreateFieldTermsMustNotBypassTheTermCapGuard(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Movie { Id = "movies/1", First = 10L, Rest = new object[] { 20L, 30L, 40L, 50L } });
            session.Store(new Movie { Id = "movies/2", First = 5L, Rest = new object[] { } });
            session.SaveChanges();
        }

        store.ExecuteIndex(new Movies_ByTag());
        Indexes.WaitForIndexing(store);

        using var session2 = store.OpenSession();

        Assert.Equal(new[] { "movies/1", "movies/2" }, session2.Advanced
            .RawQuery<Movie>("from index 'Movies/ByTag' order by Tag as long desc limit 2")
            .ToList().Select(x => x.Id).ToArray());
    }

    // the WHERE-clause streaming optimization drops the sort and streams the tree, which orders a multi-termed
    // document by whichever term it meets first. The oracle is the same query with the optimization refused.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CreateFieldTermsMustNotBypassTheStreamingGuard(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Item { Id = "items/1", Tags = new[] { "a", "z" } });
            session.Store(new Item { Id = "items/2", Tags = new[] { "m" } });
            session.SaveChanges();
        }

        store.ExecuteIndex(new Items_ByTag());
        Indexes.WaitForIndexing(store);

        Assert.Equal(
            Ids(store, "from index 'Items/ByTag' where exists(Tag) or exists(Tag) order by Tag desc"),
            Ids(store, "from index 'Items/ByTag' where exists(Tag) order by Tag desc"));
    }

    private class Items_ByTags : AbstractIndexCreationTask<Item>
    {
        public Items_ByTags()
        {
            Map = items => from i in items
                           select new
                           {
                               i.Tags
                           };
        }
    }

    // the marker means several terms on ONE entry, so a field that is always a one element array keeps the
    // streaming optimization and only a genuinely multi-termed one gives it up
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void OnlySeveralTermsOnOneEntryMakeAFieldMultiTermed(Options options)
    {
        Assert.False(FallsBackToSorting(options, new[] { "a" }, new[] { "m" }));
        Assert.True(FallsBackToSorting(options, new[] { "a", "z" }, new[] { "m" }));
    }

    // the snapshot holds an index built by 7.2.6, before the marker was recorded per entry
    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void IndexBuiltBeforeThePerEntryMarkerScansEveryTermOfAField()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        using (var file = File.Create(Path.Combine(backupPath, "RavenDB_27565.ravendb-snapshot")))
        using (var stream = typeof(RavenDB_27565).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_27565.RavenDB_27565.ravendb-snapshot"))
        {
            Assert.NotNull(stream);
            stream.CopyTo(file);
        }

        var databaseName = GetDatabaseName();
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName }))
        {
            using var session = store.OpenSession(databaseName);

            Assert.Equal(new[] { "items/1" }, session.Advanced
                .RawQuery<Item>("from index 'Items/ByCategoryAndTag' where Category = 'x' and Tag = 'z'")
                .AddParameter("rvn_corax_entry_scan", 1L)
                .ToList().Select(x => x.Id).ToArray());
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void OnlyIndexesFromVersion80OnAreTrustedToRecordTheMarkerPerEntry()
    {
        Assert.False(IndexDefinitionBaseServerSide.IndexVersion.IsPerEntryMultipleTermsMarkerSupported(IndexDefinitionBaseServerSide.IndexVersion.CoraxPerEntryMultipleTermsMarker_72));
        Assert.False(IndexDefinitionBaseServerSide.IndexVersion.IsPerEntryMultipleTermsMarkerSupported(IndexDefinitionBaseServerSide.IndexVersion.CoraxCharAsString_72));
        Assert.True(IndexDefinitionBaseServerSide.IndexVersion.IsPerEntryMultipleTermsMarkerSupported(IndexDefinitionBaseServerSide.IndexVersion.Base80Version));
    }

    private bool FallsBackToSorting(Options options, params string[][] tagsPerDocument)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            for (var i = 0; i < tagsPerDocument.Length; i++)
                session.Store(new Item { Id = $"items/{i + 1}", Tags = tagsPerDocument[i] });

            session.SaveChanges();
        }

        store.ExecuteIndex(new Items_ByTags());
        Indexes.WaitForIndexing(store);

        using var session2 = store.OpenSession();

        session2.Advanced
            .RawQuery<Item>("from index 'Items/ByTags' where exists(Tags) order by Tags include timings()")
            .Timings(out var timings)
            .ToList();

        return PlanContains(Assert.IsType<QueryInspectionNode>(timings.QueryPlan), "SortingMatch");
    }

    private static bool PlanContains(QueryInspectionNode node, string operation)
    {
        if (node.Operation.Contains(operation))
            return true;

        return node.Children != null && node.Children.Any(child => PlanContains(child, operation));
    }

    private static string[] Ids(IDocumentStore store, string rql)
    {
        using var session = store.OpenSession();
        return session.Advanced.RawQuery<Item>(rql).ToList().Select(x => x.Id).ToArray();
    }
}
