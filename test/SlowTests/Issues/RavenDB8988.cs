using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB8988 : RavenTestBase
    {
        [Fact]
        public void BetweenQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Advanced.RawQuery<dynamic>(@"
from Employees 
where HiredAt between '1992' and '1994' 
").ToList();
                }
            }
        }
    }
}
