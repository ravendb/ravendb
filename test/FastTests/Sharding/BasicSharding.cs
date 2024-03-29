﻿using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Replication;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    public class BasicSharding : RavenTestBase
    {
        public BasicSharding(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
#pragma warning disable CS0649
            public string Name;
            public string Pet;
#pragma warning restore CS0649
        }

        private class Pet
        {
#pragma warning disable CS0649
            public string Name;
            public PetType Type;
#pragma warning restore CS0649

            public enum PetType
            {
                Cat,
                Dog,
                Hamster
            }

        }

        [RavenFact(RavenTestCategory.Sharding)]
        public void CanCreateShardedDatabase()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.Null(u);
                }

            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public void CanPutCounters()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var u = new User();
                    s.Store(u, "users/1");
                    s.CountersFor(u).Increment("Likes", 555);
                    s.CountersFor(u).Increment("Views", 100);
                    s.SaveChanges();
                }

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "likes",
                                    Delta = 10
                                }
                            }
                        }
                    }
                }));

                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.Equal(565,s.CountersFor(u).Get("Likes"));
                    Assert.Equal(100,s.CountersFor(u).Get("Views"));
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public void CanPutAndGetItem()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                PutEntity(store, new DynamicJsonValue { ["Name"] = "Oren", }, "users/1");

                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.NotNull(u);
                    Assert.Equal("Oren", u.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public async Task CanPutAndGetItem2()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                PutEntity(store, new DynamicJsonValue {["Name"] = "Oren",}, "users/1");

                using (var s = store.Commands())
                {
                    var documents = await s.GetAsync(start: 0, pageSize: 100);
                    Assert.Equal(1, documents.Count());
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public void CanPutAndDeleteItem()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                PutEntity(store, new DynamicJsonValue { ["Name"] = "Oren", }, "users/1");
                string changeVector;

                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.NotNull(u);
                    Assert.Equal("Oren", u.Name);
                    changeVector = s.Advanced.GetChangeVectorFor(u);
                }

                // test delete not existing doc
                DeleteEntity(store, "users/2", changeVector: null);

                Assert.Throws<ConcurrencyException>(() => DeleteEntity(store, "users/2", changeVector));

                // test delete with concurrency exception
                var cv = changeVector.ToChangeVector();
                cv[0].Etag = 100;
                var notExpected = cv.SerializeVector();
                Assert.Throws<ConcurrencyException>(() => DeleteEntity(store, "users/1", notExpected));

                // now really delete it
                DeleteEntity(store, "users/1", changeVector);

                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.Null(u);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public void CanPutAndCheckIfExists()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                PutEntity(store, new DynamicJsonValue { ["Name"] = "Oren", }, "users/1");
                using (var s = store.OpenSession())
                {
                    Assert.True(s.Advanced.Exists("users/1"));
                }
            }
        }

        private static void PutEntity(IDocumentStore store, DynamicJsonValue user, string id)
        {
            RequestExecutor requestExecutor = store.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var blittableJsonReaderObject = context.ReadObject(user, id);
                requestExecutor.Execute(new PutDocumentCommand(requestExecutor.Conventions, id, null, blittableJsonReaderObject), context);
            }
        }

        private static void DeleteEntity(IDocumentStore store, string id, string changeVector)
        {
            RequestExecutor requestExecutor = store.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                requestExecutor.Execute(new DeleteDocumentCommand(id, changeVector), context);
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.Sharding)]
        public void CanPatch()
        {
            using (var store = Sharding.GetDocumentStore())
            using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out var context))
            {
                string id = "users/1";
                string petId = "pets/1";
                PutEntity(store, new DynamicJsonValue { ["Name"] = "Oren", }, id);
                PutEntity(store, new DynamicJsonValue
                {
                    ["Name"] = "Arava",
                    ["Type"] = "Dog"
                }, petId);

                var command = new PatchOperation.PatchCommand(store.Conventions,
                    context,
                    id,
                    null,
                    new PatchRequest
                    {
                        Script = $@"this.Pet = ""{petId}"""
                    },
                    patchIfMissing: null,
                    skipPatchIfChangeVectorMismatch: false,
                    returnDebugInformation: true,
                    test: false);

                store.GetRequestExecutor().Execute(command, context);

                using (var s = store.OpenSession())
                {
                    var user = s.Load<User>(id, b => b.IncludeDocuments(u => u.Pet));
                    var pet = s.Load<Pet>(user.Pet);
                    Assert.Equal("Arava", pet.Name);
                }
            }
        }
        
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public async Task CanPutAndGetMultipleItemsWithIncludes()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                PutEntity(store, new DynamicJsonValue
                {
                    ["Name"] = "Arava",
                    ["Type"] = "Dog"
                }, "pets/1");

                PutEntity(store, new DynamicJsonValue
                {
                    ["Name"] = "Oren",
                    ["Pet"] = "pets/1"
                }, "users/1");

                PutEntity(store, new DynamicJsonValue
                {
                    ["Name"] = "Shimaon",
                    ["Type"] = "Hamster"
                }, "pets/2");

                PutEntity(store, new DynamicJsonValue
                {
                    ["Name"] = "Tal",
                    ["Pet"] = "pets/2"
                }, "users/2");

                PutEntity(store, new DynamicJsonValue
                {
                    ["Name"] = "Potit",
                    ["Type"] = "Cat"
                }, "pets/3");

                PutEntity(store, new DynamicJsonValue
                {
                    ["Name"] = "Maxim",
                    ["Pet"] = "pets/3"
                }, "users/3");

                int shard1 = await Sharding.GetShardNumberForAsync(store, "pets/1");
                int shard2 = await Sharding.GetShardNumberForAsync(store, "pets/2");
                
                Assert.NotEqual(shard1, shard2);
                
                using (var s = store.OpenSession())
                {
                    var users = s.Load<User>(new[] { "users/1", "users/2", "users/3" }, b => b.IncludeDocuments(u => u.Pet));
                    Assert.NotNull(users);
                    Assert.Equal(3, users.Count);
                    Assert.Equal("Oren", users["users/1"].Name);
                    Assert.Equal("Tal", users["users/2"].Name);
                    Assert.Equal("Maxim", users["users/3"].Name);

                    var numberOfResults = s.Advanced.NumberOfRequests;
                    var pets = s.Load<Pet>(new[] { "pets/1", "pets/2", "pets/3" });
                    Assert.Equal("Arava", pets["pets/1"].Name);
                    Assert.Equal("Shimaon", pets["pets/2"].Name);
                    Assert.Equal("Potit", pets["pets/3"].Name);

                    Assert.Equal(numberOfResults, s.Advanced.NumberOfRequests);
                }
            }
        }
        
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public async Task MoveAndCopyAttachmentShouldThrow()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    using (var ms = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        var user = new Raven.Tests.Core.Utils.Entities.User { Name = "User 1" };
                        var user2 = new Raven.Tests.Core.Utils.Entities.User { Name = "User 1" };
                        await session.StoreAsync(user, "users/1");
                        await session.StoreAsync(user2, "users/2");
                    
                        session.Advanced.Attachments.Store(user, "foo", ms);
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Attachments.Move("users/1","foo","users/2","bar");
                    await Assert.ThrowsAsync<NotSupportedInShardingException>(() => session.SaveChangesAsync());
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Attachments.Copy("users/1","foo","users/2","bar");
                    await Assert.ThrowsAsync<NotSupportedInShardingException>(() => session.SaveChangesAsync());
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public async Task CanGenerateIdFromEmptyString()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var u = new Raven.Tests.Core.Utils.Entities.User();
                    await session.StoreAsync(u, string.Empty);
                    await session.SaveChangesAsync();
                    Assert.Equal(36, u.Id.Length);
                }
            }
        }
    }
}
