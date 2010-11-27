using Raven.Tests.Document;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
    public class QueryingOnEqualToNull : LocalClientTest
    {
        [Fact]
        public void QueryingOnEqNull()
        {
            using(var store = NewDocumentStore())
            {
                using(var s = store.OpenSession())
                {
                    s.Store(new Company
                    {
                        Phone = 1,
                        Type = Company.CompanyType.Public,
                        Name = null
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.Equal(1, s.Query<Company>().Where(x => x.Name == null).Count());
                }
            }
        }
    }
}