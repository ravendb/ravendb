using System.Linq;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11272 : RavenTestBase
    {
        public RavenDB_11272(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Create_empty_index_and_sort_by_id()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    
                    session.SaveChanges();

                    // it will create empty auto index (no entries) as users collection doesn't have 'NonExistingProperty' prop
                    // after RavenDB-14116 auto index will index everything
                    var results = session.Advanced.RawQuery<User>("from Users order by NonExistingProperty").Statistics(out var stats).ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Auto/Users/ByNonExistingProperty", stats.IndexName);

                    results = session.Advanced.RawQuery<User>("from Users order by id()").Statistics(out stats).ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Auto/Users/ByNonExistingProperty", stats.IndexName);
                }
            }
        }
    }
}
