using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests
{
    public class RavenDB_15796 : ReplicationTestBase
    {
        public RavenDB_15796(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task PutDifferentAttachmentsShouldConflict()
        {
            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver();
                }
            }))
            using (var store2 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver();
                }
            }))
            {
                await SetDatabaseId(store1, new Guid("00000000-48c4-421e-9466-000000000000"));
                await SetDatabaseId(store2, new Guid("99999999-48c4-421e-9466-999999999999"));

                using (var session = store1.OpenAsyncSession())
                {
                    var x = new User { Name = "Fitzchak" };
                    await session.StoreAsync(x, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }

                    using (var session2 = store2.OpenSession())
                    {
                        session2.Store(new User { Name = "Fitzchak" }, "users/1");
                        session2.SaveChanges();

                        using (var a2 = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                        {
                            store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a2, "a1/png"));
                        }

                        await SetupReplicationAsync(store1, store2);

                        await session.StoreAsync(new User { Name = "Toli" }, "users/2");
                        await session.SaveChangesAsync();
                        WaitForDocumentToReplicate<User>(store2, "users/2", 3000);

                        var conflicts = (await store2.Commands().GetConflictsForAsync("users/1")).ToList();
                        Assert.Equal(2, conflicts.Count);
                        var requestExecutor = store2.GetRequestExecutor();

                        using (var context = JsonOperationContext.ShortTermSingleUse())
                        using (var stringStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(_conflictedDocument)))
                        using (var blittableJson = await context.ReadForMemoryAsync(stringStream, "Reading of foo/bar"))
                        {
                            var result = new InMemoryDocumentSessionOperations.SaveChangesData((InMemoryDocumentSessionOperations)session2);
                            result.SessionCommands.Add(new PutCommandDataWithBlittableJson("users/1", null, null, blittableJson));
                            var sbc = new SingleNodeBatchCommand(DocumentConventions.Default, context, result.SessionCommands, result.Options);
                            await requestExecutor.ExecuteAsync(sbc, context);
                        }
                    }
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var conflicts = (await store2.Commands().GetConflictsForAsync("users/1")).ToList();
                    Assert.Equal(0, conflicts.Count);

                    Assert.True(await session.Advanced.Attachments.ExistsAsync("users/1", "a1"));
                }
            }
        }

        private const string _conflictedDocument = @"
        {
    ""Name"": ""Fitzchak"",
        ""LastName"": null,
        ""AddressId"": null,
        ""Count"": 0,
        ""@metadata"": {
            ""@collection"": ""Users"",
            ""Raven-Clr-Type"": ""SlowTests.Core.Utils.Entities.User, SlowTests"",
            ""@attachments"": [
            {
                ""Name"": ""a1"",
            ""Hash"": ""Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco="",
        ""ContentType"": ""a1/png"",
        ""Size"": 5
            }
            ]
        }
    }
        ";
    }
}
