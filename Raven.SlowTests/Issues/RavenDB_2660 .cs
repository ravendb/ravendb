// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2660 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.SlowTests.Issues
{
    public class RavenDB_2660 : RavenTest
    {
        public class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users => from user in users select new { user.Name };
            }
        }

        public class PeopleByName : AbstractMultiMapIndexCreationTask<Person>
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
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.DeleteIndex(new RavenDocumentsByEntityName().IndexName); // just to make sure that we won't use the precomputed batch optimization

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

                WaitForIndexing(store);

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

                WaitForIndexing(store);

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
