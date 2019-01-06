using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12059 : RavenTestBase
    {
        [Fact]
        public void Query_without_alias_should_properly_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    Assert.True(session.Advanced.RawQuery<JObject>("match (Orders as o where id() = 'orders/825-A') select o.Company").ToArray().Length > 0);
                    Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>("match (Orders as o where id() = 'orders/825-A') select Company").ToArray());

                    Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>("match (Orders as o where id() = 'orders/825-A') select o.Product, Company").ToArray());
                }
            }
        }
    }
}
