using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs
{
    public class IndexWithTwoProperties : RavenTestBase
    {
        public IndexWithTwoProperties(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void CanCreateIndexByTwoProperties()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                session.Store(new Foo { Id = "1", Value = "foo" });


                session.Store(new Foo { Id = "2", Value = "bar" });


                session.SaveChanges();
                var index = new IndexDefinitionBuilder<Foo>("FeedSync/TwoProperties")
                {

                    Map = ids => from id in ids
                        select new {id.Id, id.Value},
                }.ToIndexDefinition(new DocumentConventions());
                store.Maintenance.Send(new PutIndexesOperation(index));
            }
        }

        public class Foo 
        {
            public string Id { get; set; }
            public string Value { get; set; }
        }
    }
}