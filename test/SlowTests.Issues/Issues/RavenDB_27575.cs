using System;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27575 : RavenTestBase
{
    public RavenDB_27575(ITestOutputHelper output) : base(output)
    {
    }

    private class Movie
    {
        public string Status { get; set; }
        public string[] Genres { get; set; }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void BoostIsAppliedToInAndAllIn()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        using (var session = store.OpenSession())
        {
            for (var i = 0; i < 20; i++)
                session.Store(new Movie { Status = i < 12 ? "Released" : i < 17 ? "Rumored" : null, Genres = i % 2 == 0 ? ["Drama", "Action"] : ["Drama"] });
            session.SaveChanges();
        }

        Assert.Equal(TopScore(store, "boost(Status = 'Released', 10)"), TopScore(store, "boost(Status in ('Released'), 10)"));

        foreach (var where in new[] { "Status in ('Released', 'Rumored')", "Status in (null)", "Genres all in ('Drama', 'Action')", "(Status in ('Released') or Genres in ('Action'))" })
            Assert.True(TopScore(store, $"boost({where}, 10)") > TopScore(store, where), where);
    }

    private static double TopScore(IDocumentStore store, string where)
    {
        using var session = store.OpenSession();
        var top = session.Advanced.RawQuery<Movie>($"from Movies where {where} order by score()").WaitForNonStaleResults().First();
        return Convert.ToDouble(session.Advanced.GetMetadataFor(top)[Constants.Documents.Metadata.IndexScore]);
    }
}
