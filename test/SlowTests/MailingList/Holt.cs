using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.MailingList
{
    public class Holt : RavenTestBase
    {
        public Holt(ITestOutputHelper output) : base(output)
        {
        }

        // decimal literals, a double -> decimal conversion is correctly rounded since .NET 11 (100.01 -> 100.01000000000000511590769747)
        public static IEnumerable<object[]> Balances => new[]
        {
            new object[] { 100.0m, 100.0m, 0m },
            new object[] { 100.0m, 101m, -1m },
            new object[] { 100.0m, 100.01m, -0.01m },
            new object[] { 100.0m, 100.001m, -0.001m },
            new object[] { 100.0m, 100.0001m, -0.0001m },
            new object[] { 100.0m, 100.00001m, -0.00001m }, // fails here with System.FormatException : Input string was not in a correct format.
        };

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [MemberData(nameof(Balances))]
        public void Query_should_return_list_of_transaction_balances(decimal debit, decimal credit, decimal expected)
        {
            // Arrange
            var transaction = new TestTransaction
            {
                Debit = debit,
                Credit = credit
            };

            using (var store = Store())
            {
                using (var session = store.OpenSession())
                {
                    // Arrange
                    session.Store(transaction);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // Act
                    var query = session.Query<TransactionBalances_ByYear.Result, TransactionBalances_ByYear>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Year <= 2011).ToList();

                    // Assert
                    Assert.Equal(expected, query.First().Balance);
                }
            }
        }


        private IDocumentStore Store()
        {
            var store = GetDocumentStore();
            new TransactionBalances_ByYear().Execute(store);
            return store;
        }

        private class TestTransaction
        {
            public string GroupCompanyId { get; set; }
            public DateTime Date { get; set; }
            public string AccountId { get; set; }
            public string AccountName { get; set; }
            public decimal Debit { get; set; }
            public decimal Credit { get; set; }

            public decimal Balance
            {
                get { return Debit - Credit; }
            }
        }

        private class TransactionBalances_ByYear :
            AbstractIndexCreationTask<TestTransaction, TransactionBalances_ByYear.Result>
        {
            public TransactionBalances_ByYear()
            {
                Map = transactions => from t in transactions
                                      select new
                                      {
                                          t.GroupCompanyId,
                                          t.Date.Year,
                                          t.AccountId,
                                          t.AccountName,
                                          t.Debit,
                                          t.Credit,
                                          t.Balance
                                      };

                Reduce = results => from c in results
                                    group c by new { c.Year, c.GroupCompanyId, c.AccountId, c.AccountName }
                                        into grouping
                                    select new
                                    {
                                        grouping.Key.GroupCompanyId,
                                        grouping.Key.Year,
                                        grouping.Key.AccountId,
                                        grouping.Key.AccountName,
                                        Debit = grouping.Sum(x => x.Debit),
                                        Credit = grouping.Sum(x => x.Credit),
                                        Balance = grouping.Sum(x => x.Balance)
                                    };

                Index(x => x.GroupCompanyId, FieldIndexing.Default);
                Index(x => x.AccountId, FieldIndexing.Default);
                Index(x => x.AccountName, FieldIndexing.Default);
                Index(x => x.Year, FieldIndexing.Default);

            }

            public class Result
            {
                public string GroupCompanyId { get; set; }
                public int Year { get; set; }
                public decimal Debit { get; set; }
                public decimal Credit { get; set; }
                public decimal Balance { get; set; }
                public string AccountId { get; set; }
                public string AccountName { get; set; }
            }
        }
    }
}
