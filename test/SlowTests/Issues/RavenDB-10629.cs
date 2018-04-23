using System;
using System.Linq;
using FastTests;
using SlowTests.Core.Utils.Entities.Faceted;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10629 : RavenTestBase
    {
        [Fact]
        public void LinqToRQL_CanHandleMemberExpressionWithNullExpression()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var epsilon = TimeSpan.FromSeconds(1);
                    var now = DateTime.UtcNow;

                    var query = (from a in session.Query<Order>()
                                select new
                                {
                                    Date = DateTime.UtcNow
                                }).ToList();

                    Assert.Equal(1, query.Count);
                    Assert.True(query[0].Date - now < epsilon);
                }
            }
        }
    }
}
