using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4053 : RavenTestBase
    {
        [Fact]
        public void Index_with_custom_class_array()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new ArrayIndex());
            }
        }

        private class IndexedDoc
        {

        }

        private class SomeClass
        {

        }

        private class ArrayIndex : AbstractIndexCreationTask<IndexedDoc>
        {
            public ArrayIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Array = new SomeClass[0]
                              };
            }
        }
    }
}
