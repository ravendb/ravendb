// -----------------------------------------------------------------------
//  <copyright file="RavenDB_381.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_381 : RavenTestBase
    {
        [Fact]
        public async Task CanChangeConventionJustForOneType_Async()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.RegisterAsyncIdConvention<User>((dbName, user) => Task.FromResult("users/" + user.Name));
                }
            }))
            {
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
