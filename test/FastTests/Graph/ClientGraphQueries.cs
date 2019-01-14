using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Linq;
using Xunit;

namespace FastTests.Graph
{
    public class ClientGraphQueries : RavenTestBase
    {
        [Fact]
        public void CanGraphQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var bar = new Bar {Name = "Barvazon"};
                    var barId = "Bars/1";
                    session.Store(bar, barId);
                    session.Store(new Foo
                    {
                        Name = "Foozy",
                        Bars = new List<string> { barId }
                    });
                    session.SaveChanges();
                    FooBar res = session.Advanced.GraphQuery<FooBar>("match (Foo)-[Bars as _]->(Bars as Bar)").With("Foo",session.Query<Foo>()).Single();
                    Assert.Equal(res.Foo.Name, "Foozy");
                    Assert.Equal(res.Bar.Name, "Barvazon");
                }
            }
        }

        [Fact]
        public async Task CanAggregateQueryParametersProperlyAsync()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var bar = new Bar { Name = "Barvazon", Age = 19};
                    var barId = "Bars/1";
                    await session.StoreAsync(bar, barId);
                    await session.StoreAsync(new Foo
                    {
                        Name = "Foozy",
                        Bars = new List<string> { barId }
                    });
                    await session.SaveChangesAsync();
                    var names = new[]
                    {
                        "Fi",
                        "Fah",
                        "Foozy"
                    };
                    FooBar res = await session.Advanced.AsyncGraphQuery<FooBar>("match (Foo)-[Bars as _]->(Bars as Bar)")
                        .With("Foo", session.Query<Foo>().Where(f=> f.Name.In(names)))
                        .With("Bar",session.Query<Bar>().Where(x=>x.Age >= 18)).SingleAsync();
                    Assert.Equal(res.Foo.Name, "Foozy");
                    Assert.Equal(res.Bar.Name, "Barvazon");
                }
            }
        }
        private class FooBar
        {
            public Foo Foo { get; set; }
            public Bar Bar { get; set; }
        }
        private class Foo
        {
            public string Name { get; set; }
            public List<string> Bars { get; set; }
        }

        private class Bar
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }
    }
}
