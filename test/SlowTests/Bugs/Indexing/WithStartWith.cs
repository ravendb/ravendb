using FastTests;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Indexing
{
    public class WithStartWith : RavenTestBase
    {
        public WithStartWith(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryDocumentsFilteredByMap(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Maps = { "from doc in docs let Name = doc[\"@metadata\"][\"Name\"] where Name.StartsWith(\"Raven\") select new { Name }" },
                        Name = "test"
                    }}));

                using (var s = store.OpenSession())
                {
                    var entity = new { Name = "Ayende" };
                    s.Store(entity);
                    s.Advanced.GetMetadataFor(entity)["Name"] = "RavenDB";
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.Equal(1, s.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).Count());
                }
            }
        }

    }
}
