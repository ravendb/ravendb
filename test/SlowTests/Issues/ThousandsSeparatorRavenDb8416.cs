using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class ThousandsSeparatorRavenDb8416 : RavenTestBase
    {
        public ThousandsSeparatorRavenDb8416(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseThousandSeparatorInQueries()
        {
            using (var s = GetDocumentStore())
            {
                using (var session = s.OpenSession())
                {
                    session.Store(new
                    {
                        Age = 1_000
                    });
                    session.Store(new
                    {
                        Age = 10_000
                    });
                    session.SaveChanges();
                }
                using (var session = s.OpenSession())
                {
                    var i = session.Advanced.RawQuery<dynamic>("from @all_docs where Age > 9_999").Count();
                    Assert.Equal(1, i);
                }

            }
        }
        
    }
}
