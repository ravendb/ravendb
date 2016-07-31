// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4903.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
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
                    s.Advanced.OnSaveChangesWaitForIndexes(timeout: TimeSpan.FromSeconds(30));

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