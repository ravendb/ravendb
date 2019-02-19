using System;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12135 : RavenTestBase
    {
        [Fact]
        public void Throw_proper_error_for_alias_duplicates_in_graph_query()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e where FirstName='Nancy')-[ReportsTo]->(Employees as manager)<-[ReportsTo]-(Employees as british where Address.City='London')
                        select manager.FirstName, british.FirstName, e.FirstName                    
                    ").ToList());

                    Assert.True(e.Message.Contains("Duplicate",StringComparison.InvariantCultureIgnoreCase) &&
                                e.Message.Contains("FirstName"));
                }
            }
        }
    }
}
