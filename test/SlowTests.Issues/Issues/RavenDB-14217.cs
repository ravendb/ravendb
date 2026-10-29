using FastTests;
using Newtonsoft.Json.Linq;
using Orders;
using Tests.Infrastructure;
using Xunit;
using Raven.Client.Json.Serialization.NewtonsoftJson;

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
            using (var store = GetDocumentStore(new Options { ModifyDocumentStore = s => s.Conventions.Serialization = new NewtonsoftJsonSerializationConventions() }))
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
