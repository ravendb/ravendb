using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18473 : ReplicationTestBase
{
    public RavenDB_18473(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task MustNotThrowVoronConcurrencyErrorExceptionDuringReplication()
    {
        var file = GetTempFileName();
        try
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                // Important: the issue reproduces only when RevisionsCollectionConfiguration.PurgeOnDelete is true
                // that is the setup for "Users" collection. Let's verify that during the setup.

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, x => Assert.True(x.Collections["Users"].PurgeOnDelete));
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database, x => Assert.True(x.Collections["Users"].PurgeOnDelete));

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Foo" }, "users/1");
                    session.SaveChanges();
                }

                await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    store1.Operations.Send(new PutAttachmentOperation("users/1", "foo", profileStream, "image/png"));
                }

                using (var session = store1.OpenSession())
                {
                    var u = session.Load<User>("users/1");
                    u.Name = "karmel";
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "Foo" }, "users/1");
                    session.SaveChanges();
                }

                await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    store2.Operations.Send(new PutAttachmentOperation("users/1", "foo", profileStream, "image/png"));
                }

                using (var session = store1.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Null(user);
                }
            }
        }
        finally
        {
            File.Delete(file);
        }
    }

}
