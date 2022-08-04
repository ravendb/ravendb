// -----------------------------------------------------------------------
//  <copyright file="PhilJones.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class LazyWithIncludes : RavenTestBase
    {
        public LazyWithIncludes(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetLazyWithIncludes(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new UserByFirstName().Execute(store);
                using (var session = store.OpenSession())
                {
                    var entity = new User
                    {
                        FirstName = "Ayende"
                    };
                    session.Store(entity);

                    session.Store(new User
                    {
                        FirstName = "Rahien",
                        LastName = entity.Id
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var q = session.Query<User>()
                        .Include(x => x.LastName)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Rahien")
                        .Lazily();

                    var enumerable = q.Value.ToArray(); //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(session.Load<User>(enumerable.First().LastName));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        private class UserByFirstName : AbstractIndexCreationTask<User>
        {
            public UserByFirstName()
            {
                Map = users => from user in users
                               select new { user.FirstName };
            }
        }
    }
}
