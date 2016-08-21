using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Utils;
using SlowTests.Utils.Attributes;
using Xunit;

namespace SlowTests.MailingList
{
    public class DecimalQueries : RavenTestBase
    {
        private class Money
        {
            public decimal Amount { get; set; }
        }

        [Fact(Skip = "http://issues.hibernatingrhinos.com/issue/RavenDB-4916")]
        public async Task CanQuery()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Money
                    {
                        Amount = 10.00m
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var moneys = session.Query<Money>()
                        .Where(x => x.Amount == 10.000m)
                        .ToArray();

                    Assert.NotEmpty(moneys);
                }
            }
        }


        [Theory]
        [CriticalCultures]
        public async Task CanQueryWithOtherCulture(CultureInfo culture)
        {
            using (CultureHelper.EnsureCulture(culture))
            {
                using (var store = await GetDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Money
                        {
                            Amount = 12.34m
                        });
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var moneys = session.Query<Money>()
                            .Where(x => x.Amount == 12.34m)
                            .ToArray();

                        Assert.NotEmpty(moneys);
                    }
                }
            }
        }
    }
}
