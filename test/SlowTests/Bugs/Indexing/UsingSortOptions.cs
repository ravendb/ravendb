using FastTests;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class UsingSortOptions : RavenNewTestBase
    {
        [Fact]
        public void CanCreateIndexWithSortOptionsOnStringVal()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test", new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }" },

                    Fields =
                    {
                        {"Name", new IndexFieldOptions {Sort = SortOptions.StringVal}}
                    }
                }));

                var indexDefinition = store.Admin.Send(new GetIndexOperation("test"));

                Assert.Equal(SortOptions.StringVal, indexDefinition.Fields["Name"]?.Sort.Value);
            }
        }
    }
}
