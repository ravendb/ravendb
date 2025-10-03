using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21589 : RavenTestBase
{
    public RavenDB_21589(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void AnyWithPredicateParameterCombinedWithWhere()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 10; i++)
                    session.Store(new Number(){ Value = i + 1 });
                
                session.SaveChanges();
                
                var ravenQueryable = session.Query<Number>();

                var r1 = ravenQueryable.Where(n => n.Value < 3 || n.Value > 7).Any(n => n.Value == 4 || n.Value == 6);

                var r2 = ravenQueryable.Where(n => n.Value < 3 || n.Value > 7).Where(n => n.Value == 4 || n.Value == 6).Any();
                
                var r3 = ravenQueryable.Any(n => n.Value == 4 || n.Value == 6);
                
                Assert.Equal(false, r1);
                Assert.Equal(false, r2);
                Assert.Equal(true, r3);
            }
        }
    }

    private class Number
    {
        public int Value { get; set; }
    }
}
