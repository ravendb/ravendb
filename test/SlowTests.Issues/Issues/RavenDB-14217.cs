using FastTests;
using Newtonsoft.Json.Linq;
using Orders;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_14217 : RavenTestBase
    {
        public RavenDB_14217(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanUseAliasesOnFunctions()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Employee {FirstName = "Oren"});
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var q = s.Advanced.RawQuery<JObject>(@"
declare function r(a){
    return a;
}
from Employees 
select r(FirstName) as Name
").Single();
                    Assert.Equal("Oren", q.Value<string>("Name"));
                }
            }
        }
    }
}
