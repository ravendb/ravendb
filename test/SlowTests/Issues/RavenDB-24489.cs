using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Microsoft.IdentityModel.Tokens;
using RabbitMQ.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Json;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24489 : RavenTestBase
    {
        public RavenDB_24489(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Attachments)]
        public async Task Index_ShouldInclude_RetiredFlag_And_RetiredAt_ForRetiredAttachments()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "John" }, "users/1");
                await session.StoreAsync(new User { Name = "Johny" }, "users/2");
                await session.SaveChangesAsync();
            }

            var retireAt1 = DateTime.UtcNow.AddDays(7);
            var retireAt2 = DateTime.UtcNow.AddDays(365);
            using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("hello")))
            {
                var parameters1 = new StoreAttachmentParameters("greeting1.txt", ms) { RetireAt = retireAt1 };
                var putOp1 = new PutAttachmentOperation("users/1", parameters1);
                store.Operations.Send(putOp1);
                ms.Position = 0;
                var parameters2 = new StoreAttachmentParameters("greeting2.txt", ms) { RetireAt = retireAt2 };
                var putOp2 = new PutAttachmentOperation("users/2", parameters2);
                store.Operations.Send(putOp2);
                ms.Position = 0;
                var parameters3 = new StoreAttachmentParameters("greeting3.txt", ms) { RetireAt = retireAt2 };
                var putOp3= new PutAttachmentOperation("users/2", parameters3);
                store.Operations.Send(putOp3);
            }

            var index = new RetiredAttachmentIndex();
            await index.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenSession())
            {
                var result = session.
                    Query<RetiredAttachmentIndex.Result, RetiredAttachmentIndex>().
                    Where(x => x.RetiredAt != null && x.RetiredAt > DateTime.UtcNow.AddDays(8)).
                    ToList();

                Assert.NotNull(result);
                Assert.Single(result);
                Assert.True(result[0].Name == "Johny");
                Assert.True(result[0].Id == "users/2");

                var result2 = session.
                    Query<RetiredAttachmentIndex.Result, RetiredAttachmentIndex>().
                    Where(x => x.RetiredAt != null && x.RetiredAt > DateTime.UtcNow.AddDays(8)).
                    ProjectInto<RetiredAttachmentIndex.Result>().
                    ToList();

                Assert.NotNull(result);
                Assert.Equal(2, result2.Count);
                Assert.All(result2, x =>
                {
                    Assert.True(x.RetiredAt.HasValue, "RetiredAt should not be null");
                    Assert.True(x.RetiredAt.Value > DateTime.UtcNow.AddDays(8),
                        $"RetiredAt {x.RetiredAt} should be > now+8d");
                    Assert.False(x.Retired, "should be retired only 8 days from now");
                    Assert.StartsWith("greeting", x.Name);
                });
            }
        }

        private class RetiredAttachmentIndex : AbstractIndexCreationTask<User, RetiredAttachmentIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public bool Retired { get; set; }
                public DateTime? RetiredAt { get; set; }
            }

            public RetiredAttachmentIndex()
            {
        
                Map = users => from u in users
                    let attachments = AttachmentsFor(u) 
                    from att in attachments
                    select new Result
                    {
                        Name = att.Name,
                        RetiredAt = att.RetireAt
                    };
                StoreAllFields(FieldStorage.Yes);
            }
        }
        
        public class User
        { 
            public string Id { get; set; }

            public string Name { get; set; }
        }
    }
}
