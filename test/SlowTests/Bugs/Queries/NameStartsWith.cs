using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Queries
{
    public class NameStartsWith : RavenTestBase
    {
        public NameStartsWith(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void can_search_for_mrs_shaba(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                new User_Entity().Execute(documentStore);
                
                using (var session = documentStore.OpenSession())
                {
                    var user1 = new User { Id = @"user/111", Name = "King Shaba" };
                    session.Store(user1);

                    var user2 = new User { Id = @"user/222", Name = "Mrs. Shaba" };
                    session.Store(user2);

                    var user3 = new User { Id = @"user/333", Name = "Martin Shaba" };
                    session.Store(user3);

                    session.SaveChanges();
                }
                

                using (var session = documentStore.OpenSession())
                {
                    var result5 = session.Query<User, User_Entity>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .Where(x => x.Name.StartsWith("King S"))
                                        .ToArray();

                    Assert.Equal(1, result5.Length);

                    var result1 = session.Query<User, User_Entity>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .Where(x => x.Name.StartsWith("Mrs"))
                        .ToArray();
                    Assert.Equal(1, result1.Length);

                    var result2 = session.Query<User, User_Entity>()
                        .Customize(x => x.WaitForNonStaleResults())
                                .Where(x => x.Name.StartsWith("Mrs."))
                                .ToArray();
                    Assert.Equal(1, result2.Length);

                    var result3 = session.Query<User, User_Entity>()
                        .Customize(x => x.WaitForNonStaleResults())
                                .Where(x => x.Name.StartsWith("Mrs. S"))
                                .ToArray();
                    Assert.Equal(1, result3.Length);

                    var result4 = session.Query<User, User_Entity>()
                        .Customize(x => x.WaitForNonStaleResults())
                                .Where(x => x.Name.StartsWith("Mrs. Shaba"))
                                .ToArray();
                    Assert.Equal(1, result4.Length);

                }
            }
        }

        private class User_Entity : AbstractIndexCreationTask<User>
        {
            public User_Entity()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Id = doc.Id,
                                  Name = doc.Name,
                              };
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }

            public bool Active { get; set; }
        }

    }

}
