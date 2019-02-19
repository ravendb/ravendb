// -----------------------------------------------------------------------
//  <copyright file="RavenDB4583.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDb4583 : RavenTestBase
    {
        private static async Task CreateUsersAsync(IAsyncDocumentSession session)
        {
            var user1 = new User { Name = "Jane" };
            user1.OfficesIds.Add("office/1");
            user1.OfficesIds.Add("office/2");

            var user2 = new User { Name = "Bill" };
            user2.OfficesIds.Add("office/1");
            user2.OfficesIds.Add("office/3");

            await session.StoreAsync(user1);
            await session.StoreAsync(user2);
        }

        [Fact]
        public async Task AsyncQueriesShouldThrowTheRightException_InsteadOfUnsupportedSyncOperationException()
        {
            var query = new SomeQuery { UserId = 1 };

            using (var store = GetDocumentStore())
            {
                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    await CreateUsersAsync(session);
                    await session.SaveChangesAsync();

                    var argumentException = await Assert.ThrowsAsync<ArgumentException>(async () =>
                    {
                        var userId = "users/" + query.UserId;
                        // Act.
                        await session.Query<User>()
                            .Where(x => x.Id == userId)
                            .Where(i => i.OfficesIds.Any(o => o.In(query.OfficeIds)))
                            //.Where(i => i.OfficesIds.ContainsAny(query.OfficeIds))
                            .FirstOrDefaultAsync();
                    });
                    Assert.Contains("Value cannot be null.", argumentException.ToString());
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public IList<string> OfficesIds { get; set; }

            public User()
            {
                OfficesIds = new List<string>();
            }
        }

        private class SomeQuery
        {
            public int UserId { get; set; }
            public IEnumerable<string> OfficeIds { get; set; }

        }
    }
}