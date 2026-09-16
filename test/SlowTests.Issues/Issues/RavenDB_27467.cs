using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27467 : RavenTestBase
{
    public RavenDB_27467(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ADateShapedStringParameterMustNotBecomeTicks(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 100; i++)
                session.Store(new Movie { ReleaseDate = new DateTime(2019, 1, 1).AddDays(i * 10).ToString("yyyy-MM-dd") });

            session.SaveChanges();
        }

        new Movies_ByDate().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            foreach (var where in new[] { "ReleaseDate > $d", "ReleaseDate >= $d", "ReleaseDate < $d", "ReleaseDate between $d and $to" })
            {
                int plain = Count(where, "2020-01-01");

                Assert.True(plain > 0, $"{where}: the plain form returned nothing, the data is wrong");

                foreach (var withTime in new[] { "2020-01-01T00:00:00", "2020-01-01T00:00:00.0000000", "2020-01-01T00:00:00.0000000Z" })
                    Assert.Equal(plain, Count(where, withTime));
            }

            int Count(string where, string d) => session.Advanced
                .RawQuery<Movie>($"from index 'Movies/ByDate' where {where}")
                .AddParameter("d", d)
                .AddParameter("to", "2021-01-01")
                .ToList()
                .Count;
        }
    }

    private class Movie
    {
        public string ReleaseDate { get; set; }
    }

    private class Movies_ByDate : AbstractIndexCreationTask<Movie>
    {
        public Movies_ByDate()
        {
            Map = movies => from m in movies
                            select new { m.ReleaseDate };
        }
    }
}
