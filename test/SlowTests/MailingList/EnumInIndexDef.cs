using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class EnumInIndexDef : RavenTestBase
    {
        [Fact]
        public async Task QueryById()
        {
            using (var store = await GetDocumentStore())
            {
                new SomeDocumentIndex().Execute(store);
            }
        }

        private class SomeDocument
        {
            public string Id { get; set; }
            public string Text { get; set; }
        }

        private enum SomeEnum
        {
            Value1 = 1,
            Value2 = 2
        }

        private class SomeDocumentIndex : AbstractIndexCreationTask<SomeDocument, SomeDocumentIndex.IndexResult>
        {
            public class IndexResult
            {
                public string Id { get; set; }
                public SomeEnum SomeEnum { get; set; }
            }

            public SomeDocumentIndex()
            {
                Map = docs => from doc in docs
                              select new { Id = doc.Id, SomeEnum = SomeEnum.Value1 };

                Store(x => x.SomeEnum, FieldStorage.Yes);
            }
        }
    }
}
