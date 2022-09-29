using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19279 : RavenTestBase
    {
        public RavenDB_19279(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Saving_Attachment_In_Wrong_Document_When_Store_Docs_And_Attachment_In_SameRequest()
        {

            using var store = GetDocumentStore();
            
            using (var session = store.OpenAsyncSession())
            using (var ms = new MemoryStream(new byte[] { 1, 2, 3 }))
            using (var ms2 = new MemoryStream(new byte[] { 1, 2, 3, 4 }))
            {
                var user = new User { Name = "User 1" };
                var user2 = new User { Name = "User 2" };
                var user3 = new User { Name = "User 3" };

                await session.StoreAsync(user, "users/");
                await session.StoreAsync(user2, "users/");
                await session.StoreAsync(user3, "users/");

                session.Advanced.Attachments.Store(user, "foo", ms);
                session.Advanced.Attachments.Store(user2, "foo2", ms2);
                await Assert.ThrowsAsync<DocumentDoesNotExistException>(async () =>
                {
                    await session.SaveChangesAsync();
                });

            }
        }


        [Fact]
        public async Task Saving_Counters_In_Wrong_Document_When_Store_Docs_And_Attachment_In_SameRequest()
        {

            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "User 1" };
                var user2 = new User { Name = "User 2" };
                var user3 = new User { Name = "User 3" };

                await session.StoreAsync(user, "users/");
                await session.StoreAsync(user2, "users/");
                await session.StoreAsync(user3, "users/");

                session.CountersFor(user).Increment("HeartRate", 1);
                session.CountersFor(user2).Increment("HeartRate", 1);

                await Assert.ThrowsAsync<DocumentDoesNotExistException>(async () =>
                {
                    await session.SaveChangesAsync();
                });
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
