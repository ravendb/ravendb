using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing;

/// <summary>
/// Cost of a change of a document that an index loads with LoadDocument.
///
/// Every write to the referenced document re-maps the documents that reference it.
///
/// Three indexes load the same owner: one reads a property (Fullname), one reads nothing, one reads an array (Tags).
/// Per change: documents re-mapped (MapReferenceAttempts delta), time until not stale, managed bytes allocated.
///
/// Opt-in: RAVEN_REFERENCE_BENCH=1. Other environment variables:
///   RAVEN_REFERENCE_BENCH_COMPANIES (default 20000), RAVEN_REFERENCE_BENCH_BIG (companies of the one big owner, default 5000),
///   RAVEN_REFERENCE_BENCH_OUT (file the table is appended to, default: reference-bench.md in the temp folder),
///   RAVEN_REFERENCE_EXPECT=skip: also assert that an unrelated property re-maps nothing and a read property re-maps its fan-out,
///   RAVEN_REFERENCE_FEATURE=on: switches the feature on through <see cref="FeatureSettingKey"/>.
/// Without RAVEN_REFERENCE_EXPECT only the index results are checked and the numbers printed.
/// </summary>
public class ReferencedDocumentChangeBenchmark(ITestOutputHelper output) : RavenTestBase(output)
{
    /// <summary>Configuration key of the feature.</summary>
    private const string FeatureSettingKey = "Indexing.Static.TrackReferencedProperties";

    private const int LateCompanies = 100;

    private sealed class Owner
    {
        public string Id { get; set; }
        public string Fullname { get; set; }
        public DateTime LastLogin { get; set; }
        public string SecurityStamp { get; set; }
        public string[] Tags { get; set; }
    }

    private sealed class Company
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string OwnerId { get; set; }
    }

    private sealed class CompanyView
    {
        public string Name { get; set; }
        public string OwnerName { get; set; }
        public string[] OwnerTags { get; set; }
    }

    private sealed class CompaniesReadsOneProperty : AbstractIndexCreationTask<Company>
    {
        public CompaniesReadsOneProperty()
        {
            Map = companies => from c in companies
                let owner = LoadDocument<Owner>(c.OwnerId)
                select new { c.Name, OwnerName = owner.Fullname };
        }
    }

    private sealed class CompaniesReadsNothing : AbstractIndexCreationTask<Company>
    {
        public CompaniesReadsNothing()
        {
            Map = companies => from c in companies
                let owner = LoadDocument<Owner>(c.OwnerId)
                select new { c.Name };
        }
    }

    private sealed class CompaniesReadsArray : AbstractIndexCreationTask<Company>
    {
        public CompaniesReadsArray()
        {
            Map = companies => from c in companies
                let owner = LoadDocument<Owner>(c.OwnerId)
                select new { c.Name, OwnerTags = owner.Tags };
        }
    }

    private static readonly string[] IndexNames = [nameof(CompaniesReadsOneProperty), nameof(CompaniesReadsNothing), nameof(CompaniesReadsArray)];

    private static int Setting(string name, int fallback) => int.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : fallback;

    private sealed record Snapshot(int ReferenceAttempts, int MapAttempts);

    private sealed record Row(string Scenario, string Index, int Remapped, int MapAttempts, double Seconds, double AllocatedMb, string Expectation);

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ChangingAReferencedDocument(Options options)
    {
        if (Environment.GetEnvironmentVariable("RAVEN_REFERENCE_BENCH") != "1")
            return;

        var companies = Setting("RAVEN_REFERENCE_BENCH_COMPANIES", 20_000);
        var big = Setting("RAVEN_REFERENCE_BENCH_BIG", 5_000);
        var expect = Environment.GetEnvironmentVariable("RAVEN_REFERENCE_EXPECT") == "skip";
        const int smallOwners = 1000;
        Assert.True(big + LateCompanies < companies, "the big owner and the late companies have to fit in the companies");

        if (Environment.GetEnvironmentVariable("RAVEN_REFERENCE_FEATURE") == "on")
            options.ModifyDatabaseRecord += record => record.Settings[FeatureSettingKey] = "true";

        using var store = GetDocumentStore(options);
        new CompaniesReadsOneProperty().Execute(store);
        new CompaniesReadsNothing().Execute(store);
        new CompaniesReadsArray().Execute(store);

        // one big owner, many small owners, companies that reference an owner that does not exist yet
        using (var bulk = store.BulkInsert())
        {
            bulk.Store(new Owner { Id = "owners/big", Fullname = "Big Owner", LastLogin = new DateTime(2026, 1, 1), SecurityStamp = "stamp-0", Tags = ["a", "b"] });
            for (var i = 0; i < smallOwners; i++)
                bulk.Store(new Owner { Id = $"owners/{i}", Fullname = $"Owner {i}", LastLogin = new DateTime(2026, 1, 1), SecurityStamp = "stamp-0", Tags = ["x"] });

            for (var i = 0; i < companies; i++)
            {
                var ownerId = i < big ? "owners/big" : i < big + LateCompanies ? "owners/late" : $"owners/{i % smallOwners}";
                bulk.Store(new Company { Id = $"companies/{i}", Name = $"Company {i}", OwnerId = ownerId });
            }
        }

        var started = DateTime.UtcNow;
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(10));
        var rows = new List<Row>
        {
            new("initial build", "(all)", companies, companies, (DateTime.UtcNow - started).TotalSeconds, 0, "")
        };

        int CountOwnerName(string name)
        {
            using var session = store.OpenSession();
            return session.Query<CompanyView, CompaniesReadsOneProperty>().Count(x => x.OwnerName == name);
        }

        int CountOwnerNameNull()
        {
            using var session = store.OpenSession();
            return session.Query<CompanyView, CompaniesReadsOneProperty>().Count(x => x.OwnerName == null);
        }

        int CountOwnerTag(string tag)
        {
            using var session = store.OpenSession();
            return session.Query<CompanyView, CompaniesReadsArray>().Count(x => x.OwnerTags.Contains(tag));
        }

        // state before any change
        Assert.Equal(big, CountOwnerName("Big Owner"));
        Assert.Equal(LateCompanies, CountOwnerNameNull());
        Assert.Equal(big, CountOwnerTag("a"));

        Snapshot Take(string index)
        {
            var stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index));
            return new Snapshot(stats.MapReferenceAttempts ?? 0, stats.MapAttempts);
        }

        void Measure(string scenario, Action change, Dictionary<string, int> expectedRemapped)
        {
            var before = IndexNames.ToDictionary(x => x, Take);
            var marker = DateTime.UtcNow;
            var watch = System.Diagnostics.Stopwatch.StartNew();
            change();
            Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(10));
            watch.Stop();

            var performance = store.Maintenance.Send(new GetIndexPerformanceStatisticsOperation(IndexNames)).ToDictionary(x => x.Name, x => x.Performance);
            foreach (var index in IndexNames)
            {
                var after = Take(index);
                var remapped = after.ReferenceAttempts - before[index].ReferenceAttempts;
                var allocated = performance[index].Where(x => x.Started >= marker).Sum(x => x.AllocatedManagedBytes?.SizeInBytes ?? 0) / 1024.0 / 1024.0;
                var wanted = expectedRemapped != null && expectedRemapped.TryGetValue(index, out var w) ? w.ToString() : "";
                rows.Add(new Row(scenario, index, remapped, after.MapAttempts - before[index].MapAttempts, watch.Elapsed.TotalSeconds, allocated, wanted));

                if (expect && expectedRemapped != null && expectedRemapped.TryGetValue(index, out var exact))
                    Assert.True(remapped == exact, $"[{scenario}] {index}: {remapped} documents were re-mapped because of the reference, expected {exact}");
            }
        }

        void Update(string id, Action<Owner> change)
        {
            using var session = store.OpenSession();
            change(session.Load<Owner>(id));
            session.SaveChanges();
        }

        // S1: a property no map reads
        Measure("S1 big owner: unrelated property (LastLogin, SecurityStamp)",
            () => Update("owners/big", o => { o.LastLogin = DateTime.UtcNow; o.SecurityStamp = "stamp-1"; }),
            new() { [nameof(CompaniesReadsOneProperty)] = 0, [nameof(CompaniesReadsNothing)] = 0, [nameof(CompaniesReadsArray)] = 0 });
        Assert.Equal(big, CountOwnerName("Big Owner"));

        // S2: the property the first index reads
        Measure("S2 big owner: Fullname",
            () => Update("owners/big", o => o.Fullname = "Renamed Owner"),
            new() { [nameof(CompaniesReadsOneProperty)] = big, [nameof(CompaniesReadsNothing)] = 0, [nameof(CompaniesReadsArray)] = 0 });
        Assert.Equal(big, CountOwnerName("Renamed Owner"));
        Assert.Equal(0, CountOwnerName("Big Owner"));

        // S3: an unrelated property of a small owner
        Measure("S3 small owner: unrelated property",
            () => Update("owners/7", o => o.SecurityStamp = "stamp-1"),
            new() { [nameof(CompaniesReadsOneProperty)] = 0, [nameof(CompaniesReadsNothing)] = 0, [nameof(CompaniesReadsArray)] = 0 });

        // S4: the array the third index reads
        Measure("S4 big owner: Tags",
            () => Update("owners/big", o => o.Tags = ["a", "b", "c"]),
            new() { [nameof(CompaniesReadsOneProperty)] = 0, [nameof(CompaniesReadsNothing)] = 0, [nameof(CompaniesReadsArray)] = big });
        Assert.Equal(big, CountOwnerTag("c"));

        // S5: the owner is created after the companies that reference it
        Measure("S5 late owner is created",
            () => Update2(store, new Owner { Id = "owners/late", Fullname = "Late Owner", LastLogin = DateTime.UtcNow, SecurityStamp = "stamp-0", Tags = ["l"] }),
            new() { [nameof(CompaniesReadsOneProperty)] = LateCompanies, [nameof(CompaniesReadsArray)] = LateCompanies });
        Assert.Equal(LateCompanies, CountOwnerName("Late Owner"));
        Assert.Equal(0, CountOwnerNameNull());

        // S6: the big owner is deleted: its companies have no owner any more
        Measure("S6 big owner is deleted",
            () =>
            {
                using var session = store.OpenSession();
                session.Delete("owners/big");
                session.SaveChanges();
            },
            new() { [nameof(CompaniesReadsOneProperty)] = big, [nameof(CompaniesReadsArray)] = big });
        Assert.Equal(big, CountOwnerNameNull());
        Assert.Equal(0, CountOwnerName("Renamed Owner"));

        Report(options, companies, big, rows);
    }

    private static void Update2(IDocumentStore store, Owner owner)
    {
        using var session = store.OpenSession();
        session.Store(owner);
        session.SaveChanges();
    }

    private void Report(Options options, int companies, int big, List<Row> rows)
    {
        var text = new StringBuilder();
        text.AppendLine($"### {options.SearchEngineMode}: {companies:N0} companies, big owner {big:N0}, small owners about {(companies - big - LateCompanies) / 1000}, late companies {LateCompanies} ({DateTime.Now:yyyy-MM-dd HH:mm})");
        text.AppendLine();
        text.AppendLine("| scenario | index | re-mapped by the reference | maps | seconds | allocated MB | expected |");
        text.AppendLine("|---|---|---|---|---|---|---|");
        foreach (var row in rows)
            text.AppendLine($"| {row.Scenario} | {row.Index} | {row.Remapped:N0} | {row.MapAttempts:N0} | {row.Seconds:F2} | {row.AllocatedMb:F1} | {row.Expectation} |");

        text.AppendLine();
        output.WriteLine(text.ToString());
        File.AppendAllText(Environment.GetEnvironmentVariable("RAVEN_REFERENCE_BENCH_OUT") ?? Path.Combine(Path.GetTempPath(), "reference-bench.md"), text.ToString());
    }
}
