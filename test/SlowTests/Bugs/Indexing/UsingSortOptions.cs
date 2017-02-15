using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class UsingSortOptions : RavenTestBase
    {
        [Fact]
        public void CanCreateIndexWithSortOptionsOnStringVal()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }" },

                    Fields =
                    {
                        {"Name", new IndexFieldOptions {Sort = SortOptions.StringVal}}
                    },
                    Name = "test"
                }}));

                var indexDefinition = store.Admin.Send(new GetIndexOperation("test"));

                Assert.Equal(SortOptions.StringVal, indexDefinition.Fields["Name"]?.Sort.Value);
            }
        }
    }
}
