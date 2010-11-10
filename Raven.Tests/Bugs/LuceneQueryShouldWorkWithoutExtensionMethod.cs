using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
    public class LuceneQueryShouldWorkWithoutExtensionMethod : LocalClientTest
    {
        [Fact]
        public void CanQueryByTypeUsingLuceneQuery()
        {
            using(var store = NewDocumentStore())
            {
                using(var session = store.OpenSession())
                {
                    session.Store(new Keyword
                    {
                        Name = "foreach"
                    });
                    session.Store(new User
                    {
                        Name = "ayende"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var keywords = session.Advanced.LuceneQuery<Keyword>().WaitForNonStaleResults().ToList();
                    Assert.Equal(1, keywords.Count);
                }
            }
        }

        public class Keyword
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}