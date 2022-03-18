using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class FailingProjection : RavenTestBase
    {
        public FailingProjection(ITestOutputHelper output) : base(output)
        {
        }

        private class MyClass
        {
            public string Prop1 { get; set; }
            public string Prop2 { get; set; }
            public int Index { get; set; }
        }

        [Fact]
        public void TestFailingProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new MyClass { Index = 1, Prop1 = "prop1", Prop2 = "prop2" });
                    session.Store(new MyClass { Index = 2, Prop1 = "prop1", Prop2 = "prop2" });
                    session.Store(new MyClass { Index = 3, Prop1 = "prop1", Prop2 = "prop2" });

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var indexDefinition = new IndexDefinitionBuilder<MyClass>()
                    {
                        Map = docs => from doc in docs select new { Index = doc.Index }
                    }.ToIndexDefinition(store.Conventions);
                    indexDefinition.Name = "MyClass/ByIndex";
                    store.Maintenance.Send(new PutIndexesOperation(new[] { indexDefinition }));

                    Indexes.WaitForIndexing(store);

                    var query = session.Query<MyClass>("MyClass/ByIndex")
                    .Select(x => new MyClass
                    {
                        Index = x.Index,
                        Prop1 = x.Prop1
                    });

                    var enumerator = session.Advanced.Stream(query);
                    int count = 0;
                    while (enumerator.MoveNext())
                    {
                        Assert.IsType<MyClass>(enumerator.Current.Document);
                        Assert.Null(enumerator.Current.Document.Prop2);
                        count++;
                    }

                    Assert.Equal(3, count);
                }
            }
        }
    }
}

