using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class UsingSortOptions : RavenTestBase
    {
        [Fact]
        public void CanCreateIndexWithSortOptionsOnStringVal()
        {
            using(var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test", new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }"},

                    Fields =
                    {
                        {"Name", new IndexFieldOptions {Sort = SortOptions.StringVal} }
                    }
                });

                var indexDefinition = store.DatabaseCommands.GetIndex("test");

                Assert.Equal(SortOptions.StringVal, indexDefinition.Fields["Name"]?.Sort.Value);
            }
        }
    }
}
