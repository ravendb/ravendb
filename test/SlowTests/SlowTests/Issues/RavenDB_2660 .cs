// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2660 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_2660 : RavenTestBase
    {
        public RavenDB_2660(ITestOutputHelper output) : base(output)
        {
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users => from user in users select new { user.Name };
            }
        }

        private class PeopleByName : AbstractMultiMapIndexCreationTask<Person>
        {
            public PeopleByName()
            {
                AddMap<Person>(persons => from p in persons select new { p.Name });
                AddMap<User>(users => from u in users select new { u.Name });
            }
        }

        [Fact]
        public void ShouldCorrectlyIndexGroups()
        {
            using (var store = GetDocumentStore())
            {
                new PeopleByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10000; i++)
                    {
                        session.Store(new Person
                        {
                            Name = "Name" + i % 31
                        });
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                new UsersByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10000; i++)
                    {
                        session.Store(new User
                        {
                            Name = "Name" + i % 31
                        });
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<User, PeopleByName>().Count();

                    Assert.Equal(20000, count);

                    count = session.Query<User, UsersByName>().Count();

                    Assert.Equal(10000, count);
                }
            }
        }
    }
}
