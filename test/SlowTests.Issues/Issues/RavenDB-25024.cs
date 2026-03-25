using System;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25024 : RavenTestBase
    {
        public RavenDB_25024(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void ChangeMetadataDoesNotShowChanges()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "user1" }, id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    session.Advanced.GetMetadataFor(user)["@refresh"] = DateTime.UtcNow.AddDays(1);

                    var whatChanged = session.Advanced.WhatChanged();
                    var sessionHasChanges = session.Advanced.HasChanges;
                    var entityHasChanges = session.Advanced.HasChanged(user);

                    Assert.NotEmpty(whatChanged);
                    Assert.True(entityHasChanges);
                    Assert.True(sessionHasChanges);
                   
                }
            }
        }
    }
}
