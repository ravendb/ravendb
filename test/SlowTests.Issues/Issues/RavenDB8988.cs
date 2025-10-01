using FastTests;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RavenDB8988 : RavenTestBase
    {
        public RavenDB8988(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying)]
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
