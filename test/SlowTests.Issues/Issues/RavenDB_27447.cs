using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27447(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Doc
    {
        public string Id { get; set; }
        public string A { get; set; }
        public string B { get; set; }
    }

    private class Probe_Index : AbstractIndexCreationTask<Doc>
    {
        public Probe_Index()
        {
            Map = docs => from d in docs select new { d.A, d.B };
            Configuration[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = "true";
        }
    }

    private class ScoreOnly
    {
        public double Score { get; set; }
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void EntryScanMustNotDropClausesFromTheScore()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 100_000; i++)
                bulk.Store(new Doc { A = i % 100 == 0 ? "aaa" : "filler", B = i % 50 == 0 ? "bbb" : "other" });
        }

        new Probe_Index().Execute(store);
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(10));

        double Score(string where, int? forcedEntryScanGate = null)
        {
            using var session = store.OpenSession();
            var query = session.Advanced
                .RawQuery<ScoreOnly>($@"from index 'Probe/Index' as m where {where} order by score()
                                        select {{ Score: getMetadata(m)[""@index-score""] }} limit 1");

            if (forcedEntryScanGate.HasValue)
                query = query.AddParameter("rvn_corax_entry_scan", forcedEntryScanGate.Value);

            return query.ToList().Select(x => x.Score).First();
        }

        var a = Score("m.A = 'aaa'");
        var b = Score("m.B = 'bbb'");
        var and = Score("m.A = 'aaa' and m.B = 'bbb'");

        output.WriteLine($"a={a:G6} b={b:G6} and={and:G6}");

        // BM25 sums the per-term contributions, so an AND of two terms scores as both of them together.
        Assert.Equal(a + b, and, precision: 4);

        // Forcing the entry-scan gate must not change that: scanning filters candidates without going through the
        // matches, so the scanned clause used to contribute nothing and the score fell back to the first term alone.
        for (int gate = 0; gate < 3; gate++)
        {
            var scanned = Score("m.A = 'aaa' and m.B = 'bbb'", gate);
            output.WriteLine($"gate {gate}: {scanned:G6}");
            Assert.Equal(and, scanned, precision: 4);
        }
    }
}
