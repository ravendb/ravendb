using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11355 : RavenTestBase
    {
        private class Index1 : AbstractIndexCreationTask<Person>
        {
            public Index1()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     p.Name
                                 };
            }
        }

        [Fact]
        public void StartsWithWithNullShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session
                         .Advanced
                         .DocumentQuery<Person, Index1>()
                         .WhereStartsWith("Name", null)
                         .ToList());

                    Assert.Contains("Method startsWith() expects to get an argument of type String while it got Null", e.Message);
                }

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session
                        .Advanced
                        .RawQuery<Person>("from index 'Index1' where startsWith(Name, null)")
                        .ToList());

                    Assert.Contains("Method startsWith() expects to get an argument of type String while it got Null", e.Message);
                }
            }
        }
    }
}
