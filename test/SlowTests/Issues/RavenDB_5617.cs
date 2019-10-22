using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_5617 : RavenTestBase
    {
        public RavenDB_5617(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
        }

        private class UserByReverseName : AbstractIndexCreationTask<User>
        {
            public UserByReverseName()
            {
                Map = users => from user in users
                               select new { Name = user.Name.Reverse() };
            }
        }

        [Fact]
        public void CanAutomaticallyWaitForIndexes_ForSpecificIndex()
        {
            using (var store = GetDocumentStore())
            {
                var userByReverseName = new UserByReverseName();
                userByReverseName.Execute(store);
                using (var s = store.OpenSession())
                {
                    s.Advanced.WaitForIndexesAfterSaveChanges(
                        timeout: TimeSpan.FromSeconds(30),
                        indexes: new[] { userByReverseName.IndexName },
                        throwOnTimeout: true);

                    s.Store(new User { Name = "Oren" });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.NotEmpty(s.Query<User, UserByReverseName>().Where(x => x.Name == "nerO").ToList());
                }
            }
        }
    }
}
