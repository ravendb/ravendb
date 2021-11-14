using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using FastTests.Server.JavaScript;
using Newtonsoft.Json.Linq;
using Orders;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14217 : RavenTestBase
    {
        public RavenDB_14217(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanUseAliasesOnFunctions(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
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
