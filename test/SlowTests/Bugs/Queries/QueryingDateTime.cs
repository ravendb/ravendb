using System;
using System.Globalization;
using System.Linq;
using FastTests;
using Sparrow;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Queries
{
    public class QueryingDateTime : RavenTestBase
    {
        public QueryingDateTime(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void QueryingNonUtcTime(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var dateTime1 = DateTime.ParseExact("2011-04-08T22:00:00.0000000+02:00", DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                var dateTime2 = dateTime1.AddHours(-2);
                using (var s = store.OpenSession())
                {
                    s.Store(new Foo
                    {
                        UtcTime = dateTime1
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.Equal(1, s.Query<Foo>().Where(x => x.UtcTime >= dateTime2).Count());
                    Assert.Equal(1, s.Query<Foo>().Where(x => x.UtcTime >= dateTime1).Count());
                }
            }
        }

        private class Foo
        {
            public string Id { get; set; }
            public DateTime UtcTime { get; set; }
        }
    }
}
