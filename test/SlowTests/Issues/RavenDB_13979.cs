using System.Linq;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13979 : RavenTestBase
    {
        public RavenDB_13979(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldTranslateIdPropertyToIdFunctionInQueryWithWhereExactOverload()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Post(), "posts/1");

                    session.SaveChanges();

                    var query = session.Query<Post>()
                        .Where(x => x.Id == "posts/1", false)
                        .Select(x => new PostModel
                        {
                            Title = x.Title,

                        });

                    Assert.Equal("from 'Posts' where id() = $p0 select Title", query.ToString());

                    var postModels = query.ToList();

                    Assert.Equal(1, postModels.Count);

                    query = session.Query<Post>()
                        .Where(x => x.Id == "posts/1", true)
                        .Select(x => new PostModel
                        {
                            Title = x.Title,

                        });

                    Assert.Equal("from 'Posts' where exact(id() = $p0) select Title", query.ToString());

                    // Assert.Equal(1, query.ToList().Count); - would fail due to RavenDB-13980
                }
            }
        }

        private class PostModel
        {
            public string Title { get; set; }
        }
    }
}
