using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11442 : RavenTestBase
    {
        public class Person
        {
            public string Title { get; set; }
            public Dictionary<string, object> CustomFields { get; set; }
        }

        public class MyIndex : AbstractIndexCreationTask<Person>
        {
            public MyIndex()
            {
                Map = people =>
                    from p in people
                    select new
                    {
                        p.Title,
                        _ = p.CustomFields.Select(k => CreateField("CustomFields_" + k.Key, k.Value))
                    };
            }
        }

        [Fact]
        public void CanQueryWithCast()
        {
            using (var store = GetDocumentStore())
            {

                using (var s = store.OpenSession())
                {
                    var q = s.Query<Person, MyIndex>()
                        .Where(p => p.Title == "Super" && (int)p.CustomFields["Age"] >= 4);

                    Assert.Equal("from index 'MyIndex' where Title = $p0 and CustomFields_Age >= $p1", q.ToString());
                 
                }
            }
        }
    }
}
