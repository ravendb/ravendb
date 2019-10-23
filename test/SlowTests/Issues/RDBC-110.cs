using System.Collections.Generic;
using System.Collections.ObjectModel;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_110 : RavenTestBase
    {
        public RDBC_110(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void EntityToBlittableShouldSimplifyCollectionProperty()
        {
            using (var store = GetDocumentStore())
            {
                var asdf = new Asdf { Something1 = new[] { "value1" }, Something2 = new[] { "value2" } };
                using (var session = store.OpenSession())
                {
                    session.Store(asdf);
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Asdf>(asdf.Id);
                    session.SaveChanges();
                }
            }
        }

        private class Asdf
        {
            public string Id { get; set; }
            public IEnumerable<string> Something1 { get; set; } = new Collection<string>();
            public IEnumerable<string> Something2 { get; set; } = new HashSet<string>();

        }


    }
}
