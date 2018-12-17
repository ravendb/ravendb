using System.Linq;
using FastTests.Graph;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_12187 : RavenTestBase
    {
        [Fact(Skip = "should pass after RavenDB-12187 is resolved")]
        public void Query_with_cycle_and_where_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    WaitForUserToContinueTheTest(store);
                    var query = session.Advanced.RawQuery<JObject>(@"
                        match (Dogs as d1)-[Likes]->(Dogs as d2) 
                        where d1 != d2
                    ").ToList();
                }
            }
        }

        private void CreateData(DocumentStore store)
        {        
            using (var session = store.OpenSession())
            {
                session.Store(new Dog
                {
                    Name = "Arava",
                    Likes = new []{"Oscar"}
                });
                session.Store(new Dog
                {
                    Name = "Oscar",
                    Likes = new []{"Pheobe"}
                });
                session.Store(new Dog
                {
                    Name = "Pheobe",
                    Likes = new []{"Arava"}
                });
                session.SaveChanges();
            }
        }
    }
}
