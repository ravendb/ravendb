using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4053 : RavenTestBase
    {
        public RavenDB_4053(ITestOutputHelper output) : base(output)
        {
        }

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
