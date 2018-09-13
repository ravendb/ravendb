using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11906 : RavenTestBase
    {
        private class Index1 : AbstractIndexCreationTask<Item>
        {
            public Index1()
            {
                Map = items => from i in items
                               select new
                               {
                                   _ = CreateField(
                                       i.FieldName,
                                       i.FieldValue, new CreateFieldOptions
                                       {
                                           Indexing = FieldIndexing.Exact,
                                           Storage = i.Stored ? FieldStorage.Yes : FieldStorage.No,
                                           TermVector = null
                                       })
                               };
            }
        }

        private class Item
        {
            public string Id { get; set; }

            public string FieldName { get; set; }

            public string FieldValue { get; set; }

            public bool Stored { get; set; }
        }

        [Fact]
        public void SupportForCreateFieldWithOptions()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        FieldName = "F1",
                        FieldValue = "Value1",
                        Stored = true
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                var terms = store.Maintenance.Send(new GetTermsOperation(new Index1().IndexName, "F1", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("Value1", terms[0]);
            }
        }
    }
}
