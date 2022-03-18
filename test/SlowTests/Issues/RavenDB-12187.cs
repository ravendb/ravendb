using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12187 : RavenTestBase
    {
        public RavenDB_12187(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Query_with_cycle_and_where_should_work()
        {
            using (var store = GetDocumentStore())
            {
                Samples.CreateDataWithMultipleEdgesOfTheSameType(store);
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<JObject>(@"
                        match (Dogs as d1)-[Likes]->(Dogs as d2) 
                        where d1 != d2
                        select id(d1) as d1, id(d2) as d2
                    ").ToList();
                    Assert.Equal(3, query.Count); //sanity check

                    query = session.Advanced.RawQuery<JObject>(@"
                        match (Dogs as d1)-[Likes]->(Dogs as d2) 
                        where d1 = d2
                        select id(d1) as d1, id(d2) as d2
                    ").ToList();

                    Assert.Equal(1, query.Count);
                    Assert.Equal("dogs/2-A", query[0]["d1"].Value<string>());

                }
            }
        }
    }
}
