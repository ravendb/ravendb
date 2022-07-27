using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB8988 : RavenTestBase
    {
        public RavenDB8988(ITestOutputHelper output) : base(output)
        {
        }

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
