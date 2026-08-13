using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

/// <summary>
/// RavenDB-27045: time values are indexed under the canonical Raven date format ('2009-06-16T07:28:42.7700000'),
/// and the 'in' operator matches those terms exactly, as strings. A literal written with fewer fractional-second
/// digits ('...42.770') therefore never matched, even though '==' and 'between' accept it - those compare on ticks.
/// Ticks also ignore DateTimeKind while the indexed spelling does not (a UTC value carries a trailing 'Z'), so the
/// same mismatch shows up whenever the literal and the stored value disagree on Kind.
/// Verifies both engines and both the 'in' and 'all in' paths.
/// </summary>
public class RavenDB_27045 : RavenTestBase
{
    public RavenDB_27045(ITestOutputHelper output) : base(output)
    {
    }

    // .770 milliseconds is indexed as the canonical '.7700000'
    private static readonly DateTime DateUnspecified = new(2009, 6, 16, 7, 28, 42, 770, DateTimeKind.Unspecified);

    // the very same instant - same ticks, so the same '==' matches it - but indexed as '...7700000Z'
    private static readonly DateTime DateUtc = new(2009, 6, 16, 7, 28, 42, 770, DateTimeKind.Utc);

    private class Item
    {
        public string Id { get; set; }

        public DateTime Date { get; set; }

        public DateTime UtcDate { get; set; }

        // server-side TypeConverter turns a '07:28:42'-shaped string into a TimeSpan, and TimeSpan values mark the
        // field as holding time values too. A TimeOnly literal has the very same shape, so canonicalizing it outright
        // would stop this from matching - guarded below.
        public string Duration { get; set; }

        // A date kept as a string, which is how the issue was reported: the same TypeConverter turns it into a
        // DateTime while indexing, so the field is indexed canonically and marked as holding time values. This is
        // also the only shape that reproduces the bug through the client API - see the query below.
        public string DateAsText { get; set; }
    }

    private class Items_ByDate : AbstractIndexCreationTask<Item>
    {
        public Items_ByDate()
        {
            Map = items => from item in items
                           select new { item.Date, item.UtcDate, item.Duration, item.DateAsText };
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void InMatchesTheTimeLiteralsThatEqualityAlreadyAccepts(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Item
            {
                Id = "items/1",
                Date = DateUnspecified,
                UtcDate = DateUtc,
                Duration = "07:28:42",
                DateAsText = "2009-06-16T07:28:42.7700000"
            });
            session.SaveChanges();
        }

        new Items_ByDate().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // the canonical spellings - matched before the fix too, they guard against a regression the other way
            AssertInAgreesWithEquality(session, "Date", "2009-06-16T07:28:42.7700000");
            AssertInAgreesWithEquality(session, "UtcDate", "2009-06-16T07:28:42.7700000Z");

            // shortened fractional seconds - the reported bug
            AssertInAgreesWithEquality(session, "Date", "2009-06-16T07:28:42.770");

            // literal and stored value disagree on Kind, in both directions: '==' compares ticks and ignores that, so
            // 'in' has to consider both indexed spellings of the instant
            AssertInAgreesWithEquality(session, "UtcDate", "2009-06-16T07:28:42.770");
            AssertInAgreesWithEquality(session, "Date", "2009-06-16T07:28:42.770Z");

            // a TimeSpan-shaped term shares its spelling with a TimeOnly literal and must keep matching verbatim
            AssertInAgreesWithEquality(session, "Duration", "07:28:42");

            // a literal that resolves to a different instant must still not match
            Assert.Empty(Query(session, "where Date in ('2009-06-16T07:28:42.771')"));

            // 'in' accepts nulls alongside values - the extra terms must not choke on them
            Assert.Single(Query(session, "where Date in (null, '2009-06-16T07:28:42.770')"));

            // 'all in' path - same canonicalization, but a separate branch in both query builders. Being a
            // conjunction it stays on the single primary spelling, so a Kind-mismatched literal is not covered there.
            Assert.Single(Query(session, "where Date all in ('2009-06-16T07:28:42.770')"));
            Assert.Empty(Query(session, "where Date all in ('2009-06-16T07:28:42.771')"));
            // a single-valued field cannot hold both null and the date, but the null must not choke the term building
            Assert.Empty(Query(session, "where Date all in (null, '2009-06-16T07:28:42.770')"));

            // The same gap reached through the client API rather than RQL. It needs a string-typed property: a
            // DateTime one cannot express a shortened literal at all, because the client always serialises it through
            // GetDefaultRavenFormat, which writes all seven fractional digits.
            const string shortened = "2009-06-16T07:28:42.770";

            Assert.Single(session.Query<Item, Items_ByDate>()
                .Where(x => x.DateAsText.In(new[] { shortened }))
                .ToList());

            Assert.Single(session.Advanced.DocumentQuery<Item, Items_ByDate>()
                .WhereIn(x => x.DateAsText, new[] { shortened })
                .ToList());
        }

        // spelling out the field and the literal, otherwise a failure here only says 'the collection was empty'
        static void AssertInAgreesWithEquality(IDocumentSession session, string field, string literal)
        {
            Assert.True(Query(session, $"where {field} == '{literal}'").Count == 1, $"'==' did not match {field} '{literal}'");
            Assert.True(Query(session, $"where {field} in ('{literal}')").Count == 1, $"'in' did not match {field} '{literal}'");
        }

        static List<Item> Query(IDocumentSession session, string where) =>
            session.Advanced.RawQuery<Item>($"from index 'Items/ByDate' {where}").ToList();
    }
}
