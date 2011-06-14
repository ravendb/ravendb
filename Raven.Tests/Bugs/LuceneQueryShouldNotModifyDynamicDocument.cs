using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class LuceneQueryShouldNotModifyDynamicDocument : LocalClientTest
    {
        [Fact]
        public void CanCustomizeEntityName()
        {
            using(var store = NewDocumentStore())
            {
                using(var session = store.OpenSession())
                {
                    session.Store(new {Id = "test", Property="Test"});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Advanced.LuceneQuery<dynamic>().First();
                    Assert.Equal("Test", doc.Property);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal("Test", session.Load<dynamic>("test").Property);
                }
            }
        }
    }
}