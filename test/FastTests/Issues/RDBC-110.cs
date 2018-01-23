using System.Collections.Generic;
using System.Collections.ObjectModel;
using Xunit;

namespace FastTests.Issues
{
    public class RDBC_110 : RavenTestBase
    {
        [Fact]
        public void EntityToBlittableShouldSimplifyCollectionProperty()
        {
            using (var store = GetDocumentStore())
            {
                var asdf = new Asdf { Something = new[] { "value" } };
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
            public IEnumerable<string> Something { get; set; } = new Collection<string>();
        }


    }
}
