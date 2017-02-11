using System;
using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace NewClientTests.NewClient.Client.Indexing
{
    public class IndexingWaitForIndexes : RavenNewTestBase
    {
        public class User
        {
            public string Name { get; set; }
        }

        public class UserByName : AbstractIndexCreationTask<User>
        {
            public UserByName()
            {
                Map = users => from user in users
                    select new
                    {
                        Name = user.Name
                    };
            }
        }

        [Fact]
        public void CanAutomaticallyWaitForIndexes_ForSpecificIndex()
        {
            using (var store = GetDocumentStore())
            {
                var userByName = new UserByName();
                userByName.Execute(store);
                using (var s = store.OpenSession())
                {
                    s.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), indexes: new[] { userByName.IndexName });

                    s.Store(new User { Name = "Idan" });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.NotEmpty(s.Query<User, UserByName>().Where(x => x.Name == "Idan").ToList());
                }
            }
        }


        [Fact]
        public void CanAutomaticallyWaitForIndexes()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    // create the auto index
                    Assert.Empty(s.Query<User>().Where(x => x.Name == "Idan Haim").ToList());
                }
                using (var s = store.OpenSession())
                {
                    s.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), throwOnTimeout:false);

                    s.Store(new User { Name = "Idan Haim" });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.NotEmpty(s.Query<User>().Where(x => x.Name == "Idan Haim").ToList());
                }
            }
        }
    }
}
