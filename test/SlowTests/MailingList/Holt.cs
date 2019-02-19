using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Holt : RavenTestBase
    {
        [Theory]
        [InlineData(100.0, 100.0, 0)]
        [InlineData(100.0, 101, -1)]
        [InlineData(100.0, 100.01, -0.01)]
        [InlineData(100.0, 100.001, -0.001)]
        [InlineData(100.0, 100.0001, -0.0001)]
        [InlineData(100.0, 100.00001, -0.00001)]         // fails here with System.FormatException : Input string was not in a correct format.
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
