using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Xunit;

namespace SlowTests.Bugs.MultiMap
{
    public class Errors : RavenTestBase
    {
        [Fact]
        public void MultiMapsMustHaveSameOutput()
        {
            using(var store = GetDocumentStore())
            {
                var exception = Assert.Throws<IndexCompilationException>(() =>
                                    store.Maintenance.Send(new PutIndexesOperation(new[] {
                                                        new IndexDefinition
                                                        {
                                                            Maps =
                                                            {
                                                                "from user in docs.Users select new { user.Username }",
                                                                "from post in docs.Posts select new { post.Title }"
                                                            },
                                                            Name = "test"
                                                        }})));

                Assert.Contains(@"Map and Reduce functions of a index must return identical types.
Baseline function		: from user in docs.Users select new { user.Username }
Non matching function	: from post in docs.Posts select new { post.Title }

Common fields			: 
Missing fields			: Username
Additional fields		: Title", exception.Message);

            }            
        }

        [Fact]
        public void MultiMapsMustHaveSameOutputAsReduce()
        {
            using (var store = GetDocumentStore())
            {
                var exception = Assert.Throws<IndexCompilationException>(() => 
                                    store.Maintenance.Send(new PutIndexesOperation(new[] {
                                                        new IndexDefinition
                                                        {
                                                            Maps =
                                                            {
                                                                "from user in docs.Users select new { user.Title }",
                                                                "from post in docs.Posts select new { post.Title }"
                                                            },
                                                            Reduce = "from result in results group result by result.Title into g select new { Title = g.Key, Count = 1 }",
                                                            Name = "test"
                                                        }})));

                Assert.Contains(@"Map and Reduce functions of a index must return identical types.
Baseline function		: from user in docs.Users select new { user.Title }
Non matching function	: from result in results group result by result.Title into g select new { Title = g.Key, Count = 1 }

Common fields			: Title
Missing fields			: 
Additional fields		: Count", exception.Message);
            }
        }
    }
}
