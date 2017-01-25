using FastTests;
using Raven.NewClient.Client.Exceptions.Compilation;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class InvalidIndexes : RavenNewTestBase
    {
        [Fact(Skip = "Missing feature: RavenDB-6153")]
        public void CannotCreateIndexesUsingDateTimeNow()
        {
            using (var store = GetDocumentStore())
            {
                var ioe = Assert.Throws<IndexCompilationException>(() =>
                    store.Admin.Send(new PutIndexOperation("test",
                        new IndexDefinition
                        {

                            Maps = { @"from user in docs.Users 
                                    where user.LastLogin > DateTime.Now.AddDays(-10) 
                                    select new { user.Name}" }
                        })));

                Assert.Contains(@"Cannot use DateTime.Now during a map or reduce phase.", ioe.Message);
            }
        }

        [Fact(Skip = "Missing feature: RavenDB-6153")]
        public void CannotCreateIndexWithOrderBy()
        {
            using (var store = GetDocumentStore())
            {
                var ioe = Assert.Throws<IndexCompilationException>(() =>
                    store.Admin.Send(new PutIndexOperation("test",
                        new IndexDefinition
                        {

                            Maps = { "from user in docs.Users orderby user.Id select new { user.Name}" }
                        })));

                Assert.Contains(@"OrderBy calls are not valid during map or reduce phase, but the following was found:", ioe.Message);
            }
        }
    }
}
