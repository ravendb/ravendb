using FastTests;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Client.Exceptions.Compilation;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs.MultiMap
{
    public class Errors : RavenNewTestBase
    {
        [Fact]
        public void MultiMapsMustHaveSameOutput()
        {
            using(var store = GetDocumentStore())
            {
                var exception = Assert.Throws<IndexCompilationException>(() =>
                                    store.Admin.Send(new PutIndexOperation("test",
                                                        new IndexDefinition
                                                        {
                                                            Maps =
                                                            {
                                                                "from user in docs.Users select new { user.Username }",
                                                                "from post in docs.Posts select new { post.Title }"
                                                            }
                                                        })));

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
                                    store.Admin.Send(new PutIndexOperation("test",
                                                        new IndexDefinition
                                                        {
                                                            Maps =
                                                            {
                                                                "from user in docs.Users select new { user.Title }",
                                                                "from post in docs.Posts select new { post.Title }"
                                                            },
                                                            Reduce = "from result in results group result by result.Title into g select new { Title = g.Key, Count = 1 }"
                                                        })));

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
