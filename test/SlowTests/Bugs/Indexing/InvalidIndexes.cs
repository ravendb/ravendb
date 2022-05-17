using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Indexing
{
    public class InvalidIndexes : RavenTestBase
    {
        public InvalidIndexes(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CannotCreateIndexesUsingDateTimeNow(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var ioe = Assert.Throws<IndexCompilationException>(() =>
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {

                        Maps = { @"from user in docs.Users 
                                    where user.LastLogin > DateTime.Now.AddDays(-10) 
                                    select new { user.Name}" },
                        Name = "test"
                    })));

                Assert.Contains(@"Cannot use DateTime.Now during a map or reduce phase.", ioe.Message);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CannotCreateIndexWithOrderBy(Options options)
        {
            using (var store = GetDocumentStore())
            {
                var ioe = Assert.Throws<IndexCompilationException>(() =>
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Maps = { "from user in docs.Users orderby user.Id select new { user.Name}" },
                        Name = "test"
                    })));

                Assert.Contains(@"OrderBy calls are not valid during map or reduce phase, but the following was found:", ioe.Message);

                ioe = Assert.Throws<IndexCompilationException>(() =>
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Maps = { "docs.Users.OrderBy(user => user.Id).Select(user => new { user.Name })" },
                        Name = "test2"
                    })));

                Assert.Contains(@"OrderBy calls are not valid during map or reduce phase, but the following was found:", ioe.Message);
            }
        }
    }
}
