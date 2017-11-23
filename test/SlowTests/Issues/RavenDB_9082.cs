using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9082 : RavenTestBase
    {
        [Fact]
        public void CanUseProjectionWithArrowFunctionAndStatementBody()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                var queries = new[]
                {
                    @"from Orders as o
where id() == 'orders/830'
select {
    TopLines: o.Lines.sort((a,b)=> b.PricePerUnit - a.PricePerUnit)
        .map(a=>{ return { Name: a.ProductName } })
        .splice(0, 2)
}",
                    @"from Orders as o
where id() == 'orders/830'
select {
    Cost: o.Lines.reduce((a,l) => a + l.PricePerUnit, 0)
}"
                };

                foreach (var query in queries)
                {
                    using (var s = store.OpenSession())
                    {
                        Assert.NotEmpty(s.Advanced.RawQuery<object>(query).ToList());
                    }
                }
            }
        }
    }
}
