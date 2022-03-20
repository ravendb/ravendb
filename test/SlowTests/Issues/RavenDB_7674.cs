using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using System;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_7674 : RavenTestBase
    {
        public RavenDB_7674(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Group_by_entire_document_LastModified_formatting_issue()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByUsers().Execute(store);

                // it's going to be written as Last-Modified in document metadata
                // in order to reproduce the date needs to have at least one zero at the end
                var parsed = DateTime.Parse("2017-06-26T19:51:26.3000000").ToUniversalTime();

                var db = GetDatabase(store.Database).Result;

                db.Time.UtcDateTime = () => parsed;

                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "Joe" }, null, "users/1");
                    session.SaveChanges();
                }

                db.Time.UtcDateTime = null;

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Users_ByUsers.Result, Users_ByUsers>().ToList();

                    Assert.Equal(1, list.Count);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "Doe" }, null, "users/1");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Users_ByUsers.Result, Users_ByUsers>().ToList();

                    Assert.Equal(1, list.Count);
                }
            }
        }

        public class Users_ByUsers : AbstractIndexCreationTask<User, Users_ByUsers.Result>
        {
            public class Result
            {
                public User User { get; set; }
            }

            public Users_ByUsers()
            {
                Map = users => from user in users select new { User = user };

                Reduce = results => from result in results
                                    group result by new
                                    {
                                        User = result.User
                                    }
                                    into g
                                    select new
                                    {
                                        g.Key.User
                                    };
            }
        }
    }
}
