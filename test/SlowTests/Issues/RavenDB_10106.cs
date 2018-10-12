using System.Linq;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10106 : RavenTestBase
    {
        [Fact]
        public void StartsWithAndEndsWithShouldWorkWithExact()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Ab"
                    });

                    session.Store(new Company
                    {
                        Name = "aB"
                    });

                    session.Store(new Company
                    {
                        Name = "AB"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereStartsWith(x => x.Name, "a", exact: true)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal("aB", companies[0].Name);

                    companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereEndsWith(x => x.Name, "b", exact: true)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal("Ab", companies[0].Name);

                    companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereStartsWith(x => x.Name, "A", exact: true)
                        .ToList();

                    Assert.Equal(2, companies.Count);

                    companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereEndsWith(x => x.Name, "B", exact: true)
                        .ToList();

                    Assert.Equal(2, companies.Count);

                    companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereStartsWith(x => x.Name, "a")
                        .ToList();

                    Assert.Equal(3, companies.Count);

                    companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereEndsWith(x => x.Name, "b")
                        .ToList();

                    Assert.Equal(3, companies.Count);

                    companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereStartsWith(x => x.Name, "A", exact: true)
                        .AndAlso()
                        .WhereEndsWith(x => x.Name, "B", exact: true)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal("AB", companies[0].Name);
                }
            }
        }
    }
}
