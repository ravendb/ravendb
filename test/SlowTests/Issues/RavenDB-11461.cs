using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11461 : RavenTestBase
    {
        public class Team
        {
            public bool IsGreen;
            public bool IsYellow;
            public string Name;
        }

        [Fact]
        public void CanQueryOnTwoBools()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Team
                    {
                        IsGreen = true,
                        IsYellow = true,
                        Name = "Green And Yellow"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var r = session.Query<Team>()
                        .Where(t => t.IsYellow && t.IsGreen)
                        .ToList();

                    Assert.NotEmpty(r);
                }
            }
        }
    }
}
