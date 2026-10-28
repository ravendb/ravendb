using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    // Shapes from the second round of review of the between fold, on auto indexes. Each asserts what the two separate
    // comparisons returned before the fold existed: a folded between must never change what either bound meant on its own.
    public class BetweenFoldingReviewCases : RavenTestBase
    {
        public BetweenFoldingReviewCases(ITestOutputHelper output) : base(output)
        {
        }

        private class Money
        {
            public int Amount { get; set; }
        }

        private class Product
        {
            public string Id { get; set; }
            public string Supplier { get; set; }
            public Money Price { get; set; }
            public Money Cost { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public double Freight { get; set; }
            public DateTime OrderedAt { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("Supplier = 'suppliers/1' and Price.Amount > 10 and Cost.Amount < 20", SearchEngineMode = RavenSearchEngineMode.All)]
        public void RangesOnDifferentNestedFieldsWithTheSameLeafNameStaySeparate(Options options, string where)
        {
            using var store = GetDocumentStore(options);
            Store(store,
                new Product { Id = "products/1", Supplier = "suppliers/1", Price = new Money { Amount = 15 }, Cost = new Money { Amount = 25 } },
                new Product { Id = "products/2", Supplier = "suppliers/1", Price = new Money { Amount = 15 }, Cost = new Money { Amount = 5 } });

            Assert.Equal(new[] { "products/2" }, QueryIds<Product>(store, $"from Products where {where}"));
        }

        // a string lower bound and a numeric upper bound, or a long and a double: each bound keeps its own translation
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Lucene)]
        [RavenData("Company = 'companies/1' and Freight >= $min and Freight < $max", 10, 20.5, "orders/2", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData("Company = 'companies/1' and Freight >= $min and Freight < $max", "*", 30, "orders/1,orders/2", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void LuceneRangeBoundsOfDifferentTypesAreTranslatedIndependently(Options options, string where, object min, object max, string expectedIds)
        {
            using var store = GetDocumentStore(options);
            Store(store,
                new Order { Id = "orders/1", Company = "companies/1", Freight = 5 },
                new Order { Id = "orders/2", Company = "companies/1", Freight = 15 },
                new Order { Id = "orders/3", Company = "companies/1", Freight = 100 });

            Assert.Equal(expectedIds.Split(','), QueryIds<Order>(store, $"from Orders where {where}", ("min", min), ("max", max)));
        }

        // a long literal compares against the long term of a double field, which truncates 10.2 to 10
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData("Company = 'companies/1' and Freight > 10 and Freight < 20.5", SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void CoraxLongBoundPairedWithDoubleBoundKeepsTheLongComparison(Options options, string where)
        {
            using var store = GetDocumentStore(options);
            Store(store,
                new Order { Id = "orders/1", Company = "companies/1", Freight = 10.2 },
                new Order { Id = "orders/2", Company = "companies/1", Freight = 15 });

            Assert.Equal(new[] { "orders/2" }, QueryIds<Order>(store, $"from Orders where {where}"));
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("exact(Age = 30 and Name >= 'A' and Name < 'C')", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExactStringRangeOnAutoIndexUsesTheExactField(Options options, string where)
        {
            using var store = GetDocumentStore(options);
            Store(store,
                new User { Id = "users/1", Name = "Alice", Age = 30 },
                new User { Id = "users/2", Name = "Bob", Age = 30 },
                new User { Id = "users/3", Name = "Carl", Age = 30 });

            Assert.Equal(new[] { "users/1", "users/2" }, QueryIds<User>(store, $"from Users where {where}"));
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Lucene)]
        [RavenData("Company = 'companies/1' and OrderedAt >= $from and OrderedAt < $to", "2020-01-01T12:00:00.0000000", "2020-01-02T00:00:00.0000000", "orders/1", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData("Company = 'companies/1' and OrderedAt >= $from and OrderedAt < $to", "2020-01-03T05:00:00.0000000", "2020-01-01T03:00:00.0000000", "", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void LuceneSegmentedDateRangeMatchesTheAndOfBothBounds(Options options, string where, string from, string to, string expectedIds)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    options.ModifyDatabaseRecord?.Invoke(record);
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.QueryClauseCacheDisabled)] = "false";
                }
            });
            Store(store,
                new Order { Id = "orders/1", Company = "companies/1", OrderedAt = new DateTime(2020, 1, 1, 13, 0, 0) },
                new Order { Id = "orders/2", Company = "companies/1", OrderedAt = new DateTime(2020, 1, 2) },
                new Order { Id = "orders/3", Company = "companies/1", OrderedAt = new DateTime(2020, 1, 1, 1, 0, 0) },
                new Order { Id = "orders/4", Company = "companies/1", OrderedAt = new DateTime(2020, 1, 3, 10, 0, 0) });

            Assert.Equal(expectedIds.Split(',', StringSplitOptions.RemoveEmptyEntries), QueryIds<Order>(store, $"from Orders where {where}", ("from", from), ("to", to)));
        }

        private static void Store(IDocumentStore store, params object[] documents)
        {
            using (var session = store.OpenSession())
            {
                foreach (var document in documents)
                    session.Store(document);
                session.SaveChanges();
            }
        }

        private static string[] QueryIds<T>(IDocumentStore store, string rql, params (string Name, object Value)[] parameters)
        {
            using var session = store.OpenSession();
            IRawDocumentQuery<T> query = session.Advanced.RawQuery<T>(rql).WaitForNonStaleResults();
            foreach (var (name, value) in parameters)
                query = query.AddParameter(name, value);

            return query.ToList().Select(x => session.Advanced.GetDocumentId(x)).OrderBy(x => x).ToArray();
        }
    }
}
