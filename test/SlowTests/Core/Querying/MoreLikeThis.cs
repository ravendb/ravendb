using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.MoreLikeThis;
using SlowTests.Core.Utils.Indexes;
using Xunit;
using Address = SlowTests.Core.Utils.Entities.Address;
using Post = SlowTests.Core.Utils.Entities.Post;
using User = SlowTests.Core.Utils.Entities.User;
using Xunit.Abstractions;

namespace SlowTests.Core.Querying
{
    public class MoreLikeThis : RavenTestBase
    {
        public MoreLikeThis(ITestOutputHelper output) : base(output)
        {
        }

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

                    Indexes.WaitForIndexing(store);

                    var list = session.Query<Post, Posts_ByTitleAndContent>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == "posts/1").WithOptions(new MoreLikeThisOptions
                        {
                            MinimumDocumentFrequency = 1,
                            MinimumTermFrequency = 0
                        }))
                        .ToList();

                    Assert.Equal(3, list.Count);
                    Assert.Equal("doduck", list[0].Title);
                    Assert.Equal("prototype your idea", list[0].Desc);
                    Assert.Equal("doduck", list[1].Title);
                    Assert.Equal("love programming", list[1].Desc);
                    Assert.Equal("We do", list[2].Title);
                    Assert.Equal("prototype", list[2].Desc);
                }
            }
        }


        [Fact]
        public void CanUseMoreLikeThisWithIncludes()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = "users/1", AddressId = "addresses/1", Name = "John" });
                    session.Store(new User { Id = "users/2", AddressId = "addresses/2", Name = "John Doe" });
                    session.Store(new Address { Id = "addresses/1", City = "New York", Country = "USA" });
                    session.Store(new Address { Id = "addresses/2", City = "New York2", Country = "USA2" });
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var list = session.Query<User, Users_ByName>()
                        .Include(x => x.AddressId)
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == "users/1").WithOptions(new MoreLikeThisOptions
                        {
                            MinimumDocumentFrequency = 1,
                            MinimumTermFrequency = 0
                        }))
                        .ToList();

                    Assert.Equal(1, list.Count);
                    Assert.Equal("John Doe", list[0].Name);

                    var address = session.Load<Address>(list[0].AddressId);
                    Assert.Equal("USA2", address.Country);
                }
            }
        }
    }
}
