using System.Collections.Generic;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Server.Documents.Replication;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    public class BasicSharding : ShardedTestBase
    {
        public BasicSharding(ITestOutputHelper output) : base(output)
        {
        }

        public class User
        {
            public string Name;
            public string Pet;
        }

        public class Pet
        {
            public string Name;
            public PetType Type;
            public enum PetType
            {
                Cat,
                Dog,
                Hamster
            }

        }

        [Fact]
        public void CanCreateShardedDatabase()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.Null(u);
                }
                
            }
        }

        [Fact]
        public void CanPutAndGetItem()
        {
            using (var store = GetShardedDocumentStore())
            {
                PutEntity(store, new DynamicJsonValue {["Name"] = "Oren",}, "users/1");

                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.NotNull(u);
                    Assert.Equal("Oren", u.Name);
                }
            }
        }

        [Fact]
        public void CanPutAndDeleteItem()
        {
            using (var store = GetShardedDocumentStore())
            {
                PutEntity(store, new DynamicJsonValue {["Name"] = "Oren",}, "users/1");
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
                var notExpected  = cv.SerializeVector();
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

        [Fact]
        public void CanPutAndCheckIfExists()
        {
            using (var store = GetShardedDocumentStore())
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
                requestExecutor.Execute(new PutDocumentCommand(id, null, blittableJsonReaderObject), context);
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

        [Fact]
        public void CanPatch()
        {
            using (var store = GetShardedDocumentStore())
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

                var command = new PatchOperation.PatchCommand(
                    store.Conventions,
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

        [Fact]
        public void CanPutAndGetMultipleItemsWithIncludes()
        {
            using (var store = GetShardedDocumentStore())
            {
                PutEntity(store, new DynamicJsonValue { 
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

                using (var s = store.OpenSession())
                {
                    var users = s.Load<User>(new []{ "users/1" , "users/2", "users/3" },b => b.IncludeDocuments(u=>u.Pet));
                    Assert.NotNull(users);
                    Assert.Equal(3, users.Count);
                    Assert.Equal("Oren", users["users/1"].Name);
                    Assert.Equal("Tal", users["users/2"].Name);
                    Assert.Equal("Maxim", users["users/3"].Name);

                    var numberOfResults = s.Advanced.NumberOfRequests;
                    var pets = s.Load<Pet>(new[] {"pets/1", "pets/2", "pets/3"});
                    Assert.Equal("Arava", pets["pets/1"].Name);
                    Assert.Equal("Shimaon", pets["pets/2"].Name);
                    Assert.Equal("Potit", pets["pets/3"].Name);

                    Assert.Equal(numberOfResults, s.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
