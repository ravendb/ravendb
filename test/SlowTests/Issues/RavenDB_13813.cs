using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13813 : RavenTestBase
    {
        public RavenDB_13813(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PaymentAll());

                using (var session = store.OpenSession())
                {
                    session.Store(new Payment
                    {
                        Amount = -160.9m,
                        AmountDone = -160.9m
                    });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);
                    // WaitForUserToContinueTheTest(store);

                    var query = session.Advanced.RawQuery<PaymentAll.Result>("from index 'PaymentAll' where AmountLeft == 0 select __all_stored_fields").ToList();
                    Assert.Equal(1, query.Count);

                    var query3 = session.Advanced.RawQuery<Payment>("from index 'PaymentAll' where AmountLeft <= 0").ToList();
                    Assert.Equal(1, query3.Count);

                    var query4 = session.Advanced.RawQuery<Payment>("from index 'PaymentAll' where AmountLeft >= 0").ToList();
                    Assert.Equal(1, query4.Count);

                    var query1 = session.Advanced.RawQuery<PaymentAll.Result>("from index 'PaymentAll' where AmountLeft > 0").ToList();
                    Assert.Equal(0, query1.Count);

                    var query2 = session.Advanced.RawQuery<PaymentAll.Result>("from index 'PaymentAll' where AmountLeft < 0").ToList();
                    Assert.Equal(0, query2.Count);

                    // rawquery parameter
                    var queryWithParameter = session.Advanced.RawQuery<Payment>("from index 'PaymentAll' where AmountLeft == $pq").AddParameter("pq", 0).ToList();
                    Assert.Equal(1, queryWithParameter.Count);

                    var queryWithParameter1 = session.Advanced.RawQuery<PaymentAll.Result>("from index 'PaymentAll' where AmountLeft > $pq").AddParameter("pq", 0).ToList();
                    Assert.Equal(0, queryWithParameter1.Count);

                    var queryWithParameter2 = session.Advanced.RawQuery<PaymentAll.Result>("from index 'PaymentAll' where AmountLeft < $pq").AddParameter("pq", 0).ToList();
                    Assert.Equal(0, queryWithParameter2.Count);
                    WaitForUserToContinueTheTest(store);
                    // dont work:
                    var result = session.Query<PaymentAll.Result, PaymentAll>().Where(a => a.AmountLeft < 0).ProjectInto<PaymentAll.Result>().ToList();
                    Assert.Equal(0, result.Count);

                    var result1 = session.Query<PaymentAll.Result, PaymentAll>().Where(a => a.AmountLeft <= 0).ProjectInto<PaymentAll.Result>().ToList();
                    Assert.Equal(1, result1.Count);

                    var result2 = session.Query<PaymentAll.Result, PaymentAll>().Where(a => a.AmountLeft >= 0).ProjectInto<PaymentAll.Result>().ToList();
                    Assert.Equal(1, result2.Count);

                    var result3 = session.Query<PaymentAll.Result, PaymentAll>().Where(a => a.AmountLeft > 0).ProjectInto<PaymentAll.Result>().ToList();
                    Assert.Equal(0, result3.Count);

                    var result4 = session.Query<PaymentAll.Result, PaymentAll>().Where(a => a.AmountLeft == 0).ProjectInto<PaymentAll.Result>().ToList();
                    Assert.Equal(1, result4.Count);
                }
            }
        }
        public class Payment
        {
            public string Id { get; set; }
            public decimal Amount { get; set; }
            public decimal AmountDone { get; set; }
        }

        private class PaymentAll : AbstractIndexCreationTask<Payment>
        {
            public class Result
            {
                public string Id { get; set; }
                public decimal AmountLeft { get; set; }
            }

            public PaymentAll()
            {
                Map = results =>
                    from result in results
                    select new Result
                    {
                        Id = result.Id,
                        AmountLeft = result.Amount - result.AmountDone
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
