// -----------------------------------------------------------------------
//  <copyright file="RavenDB_381.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Util;
using Raven.Client.Document;

using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_381 : RavenTestBase
    {
        [Fact]
        public void CanChangeConventionJustForOneType()
        {
            using(var store = GetDocumentStore())
            {
                store.Conventions.RegisterIdConvention<User>((dbName, cmds, user) => "users/" + user.Name);

                using(var session = store.OpenSession())
                {
                    var entity = new User {Name = "Ayende"};
                    session.Store(entity);

                    Assert.Equal("users/Ayende", session.Advanced.GetDocumentId(entity));
                }
            }
        }

        [Fact]
        public async Task CanChangeConventionJustForOneType_Async()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.RegisterAsyncIdConvention<User>((dbName, cmds, user) => new CompletedTask<string>("users/" + user.Name));

                using (var session = store.OpenAsyncSession())
                {
                    var entity = new User { Name = "Ayende" };
                    await session.StoreAsync(entity);
                    await session.SaveChangesAsync();
                    Assert.Equal("users/Ayende", session.Advanced.GetDocumentId(entity));
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
