using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

/// <summary>
/// RavenDB-27045: time values are indexed under the canonical Raven date format ('2009-06-16T07:28:42.7700000'),
/// and the 'in' operator matches time terms exactly, as strings. A literal written with fewer fractional-second
/// digits ('...42.770') therefore never matched, even though '==' and 'between' accept it - those compare on ticks.
/// Verifies both engines and both the 'in' and 'all in' paths.
/// </summary>
public class RavenDB_27045 : RavenTestBase
{
    public RavenDB_27045(ITestOutputHelper output) : base(output)
    {
    }

    // .770 milliseconds is indexed as the canonical '.7700000'.
    private static readonly DateTime Date = new DateTime(2009, 6, 16, 7, 28, 42, 770);

    private class Item
    {
        public string Id { get; set; }

        public DateTime Date { get; set; }
    }

    private class Items_ByDate : AbstractIndexCreationTask<Item>
    {
        public Items_ByDate()
        {
            Map = items => from item in items
                           select new { item.Date };
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void InMatchesTimeLiteralWithFewerFractionalDigits(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Item { Id = "items/1", Date = Date });
            session.SaveChanges();
        }

        new Items_ByDate().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // 'in' path
            // the canonical form - matched before the fix too, guards against a regression the other way
            Assert.Single(Query(session, "where Date in ('2009-06-16T07:28:42.7700000')"));
            // shortened fractional seconds - the reported bug, never matched before the fix
            Assert.Single(Query(session, "where Date in ('2009-06-16T07:28:42.770')"));
            // a literal that resolves to a different instant must still not match
            Assert.Empty(Query(session, "where Date in ('2009-06-16T07:28:42.771')"));
            // 'in' accepts nulls alongside values - canonicalization must not choke on them
            Assert.Single(Query(session, "where Date in (null, '2009-06-16T07:28:42.770')"));

            // 'all in' path - same canonicalization, but a separate branch in both query builders
            Assert.Single(Query(session, "where Date all in ('2009-06-16T07:28:42.770')"));
            Assert.Empty(Query(session, "where Date all in ('2009-06-16T07:28:42.771')"));
            // a single-valued field can't contain both null and the date, but the null must not choke canonicalization
            Assert.Empty(Query(session, "where Date all in (null, '2009-06-16T07:28:42.770')"));
        }

        static List<Item> Query(IDocumentSession session, string where) =>
            session.Advanced.RawQuery<Item>($"from index 'Items/ByDate' {where}").ToList();
    }
}
