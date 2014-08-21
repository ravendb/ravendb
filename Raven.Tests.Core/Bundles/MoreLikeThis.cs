using Raven.Abstractions.Data;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using System.Linq;
using Xunit;

namespace Raven.Tests.Core.Bundles
{
    public class MoreLikeThis : RavenCoreTestBase
    {
        [Fact]
        public void CanUseBasicMoreLikeThis()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Posts_ByTitleAndContent();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Post { Id = "posts/1", Title = "doduck", Desc = "prototype" });
                    session.Store(new Post { Id = "posts/2", Title = "doduck", Desc = "prototype your idea" });
                    session.Store(new Post { Id = "posts/3", Title = "doduck", Desc = "love programming" });
                    session.Store(new Post { Id = "posts/4", Title = "We do", Desc = "prototype" });
                    session.Store(new Post { Id = "posts/5", Title = "We love", Desc = "challange" });
                    session.SaveChanges();

                    WaitForIndexing(store);

                    var list = session.Advanced.MoreLikeThis<Post, Posts_ByTitleAndContent>(new MoreLikeThisQuery 
                        { 
                            DocumentId = "posts/1",
                            MinimumDocumentFrequency = 1,
                            MinimumTermFrequency = 0
                        });

                    Assert.Equal(3, list.Length);
                    Assert.Equal("doduck", list[0].Title);
                    Assert.Equal("prototype your idea", list[0].Desc);
                    Assert.Equal("doduck", list[1].Title);
                    Assert.Equal("love programming", list[1].Desc);
                    Assert.Equal("We do", list[2].Title);
                    Assert.Equal("prototype", list[2].Desc);
                }
            }
        }
    }
}
