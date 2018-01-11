using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10143 : RavenTestBase
    {
        [Fact]
        public void Dynamic_select_distinct_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Arek"
                    });
                    
                    session.Store(new User()
                    {
                        Name = "Arek"
                    });
                    
                    session.SaveChanges();

                    var results = session.Query<User>().Select(x => x.Name).Distinct().ToList();
                    
                    Assert.Equal(1, results.Count);
                }
            }
        }
    }
}
