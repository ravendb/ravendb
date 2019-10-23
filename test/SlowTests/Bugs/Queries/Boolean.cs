using FastTests;
using Xunit;
using System.Linq;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Queries
{
    public class Boolean : RavenTestBase
    {
        public Boolean(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryOnNegation()
        {
            using(var store = GetDocumentStore())
            {
                using(var s = store.OpenSession())
                {
                    s.Store(new User{Active = false});
                    s.SaveChanges();	
                }

                using(var s = store.OpenSession())
                {
                    Assert.Equal(1, s.Query<User>()
                        .Where(x => !x.Active)
                        .Count());
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }
    }
}
