using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.ServerWide;
using SlowTests.Core.Utils.Entities;
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
                    var x = new User {Name = "Fitzchak"};
                    await session.StoreAsync(x, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }

                    using (var session2 = store2.OpenAsyncSession())
                    {
                        await session2.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                        await session2.SaveChangesAsync();

                        using (var a2 = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                        {
                            store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a2, "a1/png"));
                        }
                        await SetupReplicationAsync(store1, store2);
                    }
                }
                WaitForUserToContinueTheTest(store2);
            }
        }
    }
}
