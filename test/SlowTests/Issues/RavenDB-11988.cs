using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11988 : RavenTestBase
    {
        [Fact]
        public async Task QueryWithInvocationExpressionInsideWhereShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new DomainModel
                    {
                        BrandId = "BY",
                        Description = "LLC"
                    });
                    s.Store(new DomainModel
                    {
                        BrandId = "XYZ",
                        Description = "LLC"
                    });
                    s.SaveChanges();
                }

                Func<DomainModel, bool> filter = this.GetFilter();
                Expression<Func<DomainModel, bool>> expr = _ => filter(_);

                using (var session = store.OpenAsyncSession())
                {
                    var query = session
                        .Query<DomainModel>()
                        .Statistics(out QueryStatistics stats)
                        .Where(expr)
                        .Search(_ => _.Description, "LLC");

                    var ex = await Assert.ThrowsAsync<NotSupportedException>(async () => await query.ToListAsync());
                    Assert.Contains("Invocation expressions such as Where(x => SomeFunction(x)) are not supported in RavenDB queries", ex.InnerException.Message);
                }
            }
        }

        [Fact]
        public void ToStringOnErrounousQueryInspectorShouldNotCauseStackOverflow()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new DomainModel
                    {
                        BrandId = "BY",
                        Description = "LLC"
                    });
                    s.Store(new DomainModel
                    {
                        BrandId = "XYZ",
                        Description = "LLC"
                    });
                    s.SaveChanges();
                }

                Func<DomainModel, bool> filter = this.GetFilter();
                Expression<Func<DomainModel, bool>> expr = _ => filter(_);

                using (var session = store.OpenSession())
                {
                    var query = session
                        .Query<DomainModel>()
                        .Statistics(out QueryStatistics stats)
                        .Where(expr)
                        .Search(_ => _.Description, "LLC");

                    var ex = Assert.Throws<NotSupportedException>(() => query.ToString());
                    Assert.Contains("Invocation expressions such as Where(x => SomeFunction(x)) are not supported in RavenDB queries", ex.InnerException.Message);
                }
            }
        }

        private class DomainModel
        {
            public string BrandId { get; set; }
            public string Description { get; set; }
        }

        private Func<DomainModel, bool> GetFilter()
        {
            Func<DomainModel, bool> filter = _ => _.BrandId == "BY";
            return filter;
        }
    }
}
