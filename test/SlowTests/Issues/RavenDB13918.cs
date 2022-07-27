using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB13918 : RavenTestBase
    {
        public RavenDB13918(ITestOutputHelper output) : base(output)
        {
        }

        public class Transaction
        {
            public string Debtor, Creditor;
            public decimal Amount;
            public string Currency;
        }

        public class Index : AbstractMultiMapIndexCreationTask<Transaction>
        {
            public Index()
            {
                AddMap<Transaction>(c =>
                    from t in c
                    select new { Amount = -t.Amount, Debtor = t.Creditor, Creditor = t.Debtor, t.Currency,  }
                );

                AddMap<Transaction>(c =>
                    from t in c
                    select new {Creditor = t.Creditor, Debtor = t.Debtor, Currency = t.Currency, Amount = t.Amount}
                );


                Reduce = results => from transaction in results
                    group transaction by new {transaction.Creditor, transaction.Currency, transaction.Debtor}
                    into g
                    select new {g.Key.Debtor, g.Key.Creditor, g.Key.Currency, Amount = g.Sum(x => x.Amount)};
            }
        }

        [Fact]
        public void NegationInIndex()
        {
            using (var store = GetDocumentStore())
            {
                new Index().Execute(store);

                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 3; i++)
                    {
                        s.Store(new Transaction
                        {
                            Amount = 124.45M,
                            Creditor = "creditor",
                            Debtor = "debtor",
                            Currency = "EURO"
                        });
                    }

                    s.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var result = s.Query<Transaction, Index>().OrderBy(x=>x.Amount).ToList();
                    Assert.Equal(-373.35M, result[0].Amount);
                    Assert.Equal(373.35M, result[1].Amount);


                }
            }
        }
    }
}
