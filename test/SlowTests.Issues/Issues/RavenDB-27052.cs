using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27052 : RavenTestBase
    {
        public RavenDB_27052(ITestOutputHelper output) : base(output)
        {
        }

        private class Event
        {
            public string Id { get; set; }

            public DateTimeOffset ControllerResultDateTime { get; set; }
        }

        private class Events_ByControllerResultDateTime : AbstractIndexCreationTask<Event>
        {
            public Events_ByControllerResultDateTime()
            {
                Map = events => from e in events
                                select new
                                {
                                    e.ControllerResultDateTime
                                };

                // Exact indexing forces the KeywordAnalyzer and, on Lucene, stores the DateTimeOffset
                // via the round-trip "o" format (offset preserved, 7 fractional digits) with no numeric
                // _Time companion field.
                Index(x => x.ControllerResultDateTime, FieldIndexing.Exact);
            }
        }

        // The document stores a DateTimeOffset with a non-zero offset and no fractional seconds:
        //   2026-07-10T15:24:46+02:00
        // Lucene + Exact indexes the term as the round-trip form "2026-07-10T15:24:46.0000000+02:00".
        // Corax normalizes any DateTimeOffset to UTC ("...Z") regardless of Exact.
        private static readonly DateTimeOffset Value = new DateTimeOffset(2026, 7, 10, 15, 24, 46, TimeSpan.FromHours(2));

        // The value the way a client would naturally write it - offset preserved, no fractional part.
        private const string QueryWithoutFractionalSeconds = "2026-07-10T15:24:46+02:00";

        // The exact round-trip ("o") form that Lucene actually stores as the index term.
        private const string QueryWithFractionalSeconds = "2026-07-10T15:24:46.0000000+02:00";

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Exact_query_on_DateTimeOffset_should_be_consistent_across_engines(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Events_ByControllerResultDateTime().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Event { ControllerResultDateTime = Value });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // The full round-trip form matches on both engines.
                AssertCount(store, QueryWithFractionalSeconds, expected: 1);

                // The natural form (no fractional seconds) must also match on both engines. exact() on a date field
                // matches by instant (UTC ticks), so the offset and the missing fractional part are irrelevant.
                // Before the fix (IndexVersion < ExactDatesUseTimeTicks_62) this returned 0 on Lucene because the
                // stored Exact term kept the ".0000000+02:00" round-trip form and exact bypassed the tick-based match.
                AssertCount(store, QueryWithoutFractionalSeconds, expected: 1);
            }
        }

        private static void AssertCount(IDocumentStore store, string value, int expected)
        {
            using (var session = store.OpenSession())
            {
                var count = session.Advanced
                    .RawQuery<Event>($"from index '{new Events_ByControllerResultDateTime().IndexName}' where exact(ControllerResultDateTime == $p0)")
                    .AddParameter("p0", value)
                    .ToList()
                    .Count;

                Assert.Equal(expected, count);
            }
        }
    }
}
