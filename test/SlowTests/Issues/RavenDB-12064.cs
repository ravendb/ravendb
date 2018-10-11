using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12064 : RavenTestBase
    {
        [Fact]
        public void ShouldGetIdentityPropertyFromFilteredType()
        {
            using (var store = GetDocumentStore())
            {
                new UsersIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<UsersIndex.Result, UsersIndex>()
                        .OfType<User>()
                        .Select(u => u.Id);

                    Assert.Equal("from index 'indexes/users/default' select id() as Id", query.ToString());

                    var ids = query.ToList();

                    Assert.NotNull(ids[0]);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class UsersIndex : AbstractIndexCreationTask<User, UsersIndex.Result>
        {
            public override string IndexName => "indexes/users/default";

            public UsersIndex()
            {
                Map = users => from user in users
                    select new Result
                    {
                        Name = user.Name
                    };
            }

            public class Result
            {
                public string Name { get; set; }
            }
        }

    }
}
