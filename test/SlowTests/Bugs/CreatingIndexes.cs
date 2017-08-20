using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Bugs
{
    public class CreatingIndexes : RavenTestBase
    {
        private class AllDocs1 : AbstractIndexCreationTask<object>
        {
            public AllDocs1()
            {
                Map = docs => from doc in docs select new { x = 1 };
            }
        }

        private class AllDocs2 : AbstractIndexCreationTask<object>
        {
            public AllDocs2()
            {
                Map = docs => from doc in docs select new { x = 2 };
            }
        }

        [Fact]
        public void CanCreateIndexes()
        {
            using (var store = GetDocumentStore())
            {
                IndexCreation.CreateIndexes(new AbstractIndexCreationTask[] { new AllDocs1(), new AllDocs2() }, store);
            }
        }
    }
}
