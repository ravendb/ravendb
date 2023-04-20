using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding.Client.Session
{
    public class BasicShardedSessionTests : RavenTestBase
    {
        public BasicShardedSessionTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Should_Put_Documents_To_The_Same_Shard(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                const string companyId = "Companies/1";
                const string orderId = $"orders/1${companyId}";

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Acme Inc."
                    }, companyId);

                    session.Store(new Order
                    {
                        Id = orderId,
                        Company = companyId
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(orderId, builder => builder.IncludeDocuments(o => o.Company));
                    Assert.NotNull(order);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.True(session.Advanced.IsLoaded(companyId));
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public void TwoDigitShard()
        {
            using (var store = Sharding.GetDocumentStore(shards: new Dictionary<int, DatabaseTopology>()
                       {
                           { 1, new DatabaseTopology() },
                           { 12, new DatabaseTopology() },
                           { 2, new DatabaseTopology() }
                       }
                   ))
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "user1" }, "users/1");
                    var user2 = new User { Name = "user2", Age = 1 };
                    newSession.Store(user2, "users/2");
                    var user3 = new User { Name = "user3", Age = 1 };
                    newSession.Store(user3, "users/3");
                    newSession.Store(new User { Name = "user4" }, "users/4");

                    newSession.Delete(user2);
                    user3.Age = 3;
                    newSession.SaveChanges();

                    var tempUser = newSession.Load<User>("users/2");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/3");
                    Assert.Equal(tempUser.Age, 3);
                    var user1 = newSession.Load<User>("users/1");
                    var user4 = newSession.Load<User>("users/4");

                    newSession.Delete(user4);
                    user1.Age = 10;
                    newSession.SaveChanges();

                    tempUser = newSession.Load<User>("users/4");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/1");
                    Assert.Equal(tempUser.Age, 10);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public void CanUseIdentities()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var entity = new User
                    {
                        LastName = "Adi"
                    };

                    s.Store(entity, "users|");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var entity = new User
                    {
                        LastName = "Avivi"
                    };

                    s.Store(entity, "users|");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var entityWithId1 = s.Load<User>("users/1");
                    var entityWithId2 = s.Load<User>("users/2");

                    Assert.NotNull(entityWithId1);
                    Assert.NotNull(entityWithId2);

                    Assert.Equal("Adi", entityWithId1.LastName);
                    Assert.Equal("Avivi", entityWithId2.LastName);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public async Task CanRetryServerSideId()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var orchestrator = Sharding.GetOrchestrator(store.Database);
                orchestrator.ForTestingPurposesOnly();

                var u = new User { Name = "foo" };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(u, "users/");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(u.Id);
                    Assert.Equal("foo", loaded.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching | RavenTestCategory.Sharding)]
        public async Task CanRetryBatchPatch()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var orchestrator = Sharding.GetOrchestrator(store.Database);
                orchestrator.ForTestingPurposesOnly();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/2");
                    await session.StoreAsync(new User { Name = "Oren" }, "users/3");
                    await session.StoreAsync(new User { Name = "Aviv" }, "users/4");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var ids = new List<string>
                    {
                        "users/1",
                        "users/3"
                    };

                    session.Advanced.Defer(new BatchPatchCommandData(ids, new PatchRequest
                    {
                        Script = "this.Name = 'test';"
                    }, null));

                    session.Advanced.Defer(new BatchPatchCommandData(new List<string> { "users/4" }, new PatchRequest
                    {
                        Script = "this.Name = 'test2';"
                    }, null));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u1 = await session.LoadAsync<User>("users/1");
                    var u2 = await session.LoadAsync<User>("users/2");
                    var u3 = await session.LoadAsync<User>("users/3");
                    var u4 = await session.LoadAsync<User>("users/4");

                    Assert.Equal("test", u1.Name);
                    Assert.Equal("Karmel", u2.Name);
                    Assert.Equal("test", u3.Name);
                    Assert.Equal("test2", u4.Name);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u2 = await session.LoadAsync<User>("users/2");

                    session.Advanced.Defer(new BatchPatchCommandData(new List<(string Id, string ChangeVector)> { (u2.Id, "invalidCV") }, new PatchRequest
                    {
                        Script = "this.Name = 'test2';"
                    }, null));

                    await Assert.ThrowsAsync<ConcurrencyException>(() => session.SaveChangesAsync());
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Patching | RavenTestCategory.Sharding)]
        public async Task CanRetryAttachment()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var orchestrator = Sharding.GetOrchestrator(store.Database);
                orchestrator.ForTestingPurposesOnly();

                var names = new[]
                {
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt",
                    "profile.png",
                };
                var u = new User { Name = "foo" };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/2");
                    await session.StoreAsync(new User { Name = "Oren" }, "users/3");
                    await session.StoreAsync(u,"users/");

                    await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                    await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                    {
                        session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                        session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                        session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < names.Length; i++)
                    {
                        var user = await session.LoadAsync<User>("users/" + (i + 1));
                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                        var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                        Assert.Equal(1, attachments.Length);
                        var attachment = attachments[0];
                        Assert.Equal(names[i], attachment.GetString(nameof(AttachmentName.Name)));
                        var hash = attachment.GetString(nameof(AttachmentName.Hash));
                        if (i == 0)
                        {
                            Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                            Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                        else if (i == 1)
                        {
                            Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                            Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                        else if (i == 2)
                        {
                            Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                            Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Sharding)]
        [InlineData(null)]
        [InlineData("")]
        public async Task CanStoreNullOrEmptyId(string id)
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {

                    var obj = new { Product = "Milk", Total = new decimal(1.1), Region = 1 };

                    await session.StoreAsync(obj, id);
                    session.Advanced.GetMetadataFor(obj)["@collection"] = "Orders";

                    await session.SaveChangesAsync();
                }
            }
        }
    }
}
