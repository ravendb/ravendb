using System.Linq;
using System.Reflection;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Bugs
{
    public class CreatingIndexes : RavenTestBase
    {
        public class AllDocs1 : AbstractIndexCreationTask<object>
        {
            public AllDocs1()
            {
                Map = docs => from doc in docs select new { x = 1 };
            }
        }

        public class AllDocs2 : AbstractIndexCreationTask<object>
        {
            public AllDocs2()
            {
                Map = docs => from doc in docs select new { x = 2 };
            }
        }

        [Fact]
        public void CanCreateIndexes()
        {
            using(var store = GetDocumentStore())
            {
                var assembly = new AssemblyName(typeof(CreatingIndexes).GetTypeInfo().Assembly.FullName);
                IndexCreation.CreateIndexes(Assembly.Load(assembly), store, new [] { typeof(AllDocs1), typeof(AllDocs2) });
            }
        }
    }
}
