// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4903.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4903 : RavenTest
    {
        public class User
        {
            public string Name { get; set; }
        }

        public class UserByReverseName : AbstractIndexCreationTask<User>
        {
            public UserByReverseName()
            {
                Map = users => from user in users
                    select new {Name = user.Name.Reverse()};
            }
        }

        [Fact]
        public void CanAutomaticallyWaitForIndexes_ForSpecificIndex()
        {
            using (var store = NewDocumentStore())
            {
                var userByReverseName = new UserByReverseName();
                userByReverseName.Execute(store);
                using (var s = store.OpenSession())
                {
                    s.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), indexes:new [] {userByReverseName.IndexName});

                    s.Store(new User { Name = "Oren" });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.NotEmpty(s.Query<User, UserByReverseName>().Where(x => x.Name == "nerO").ToList());
                }
            }
        }


        [Fact]
        public void CanAutomaticallyWaitForIndexes()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    // create the auto index
                    Assert.Empty(s.Query<User>().Where(x => x.Name == "Oren").ToList());
                }
                using (var s = store.OpenSession())
                {
                    s.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(30));

                    s.Store(new User {Name = "Oren"});

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.NotEmpty(s.Query<User>().Where(x => x.Name == "Oren").ToList());
                }
            }
        }
    }
}