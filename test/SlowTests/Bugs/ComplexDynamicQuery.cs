using FastTests;
using Xunit;
using System.Linq;

namespace SlowTests.Bugs
{
    public class ComplexDynamicQuery : RavenTestBase
    {
        [Fact]
        public void UsingNestedCollections()
        {
            using(var store = GetDocumentStore())
            {
                using(var s = store.OpenSession())
                {
                    s.Advanced
                        .DocumentQuery<User>()
                        .Where("Widgets,Sprockets,Name", "Sprock01")
                        .ToList();
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
