using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Indexing
{
    public class CanIndexWithCharLiteral : RavenTestBase
    {
        public CanIndexWithCharLiteral(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryDocumentsIndexWithCharLiteral(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs select  new { SortVersion = doc.Version.PadLeft(5, '0') }" },
                    Fields = { { "SortVersion", new IndexFieldOptions { Storage = FieldStorage.Yes } } },
                    Name = "test"
                }}));
                
                using (var s = store.OpenSession())
                {
                    var entity = new { Version = "1" };
                    s.Store(entity);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.Equal(1, s.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).Count());
                    Assert.Equal("00001", s.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).ProjectInto<Result>().First().SortVersion);
                }
            }
        }

        private class Result
        {
            public string SortVersion { get; set; }
        }
    }
}
