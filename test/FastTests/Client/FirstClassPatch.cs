using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class FirstClassPatch : RavenTestBase
    {
        public FirstClassPatch(ITestOutputHelper output) : base(output)
        {
        }

        private string _docId = "users/1-A";

        private class User
        {
            public Stuff[] Stuff { get; set; }
            public DateTime LastLogin { get; set; }
            public int[] Numbers { get; set; }
        }

        private class Stuff
        {
            public int Key { get; set; }
            public string Phone { get; set; }
            public Pet Pet { get; set; }
            public Friend Friend { get; set; }
            public Dictionary<string, string> Dic { get; set; }
        }

        private class Friend
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public Pet Pet { get; set; }
        }

        private class Pet
        {
            public string Name { get; set; }
            public string Kind { get; set; }
        }

        private class Customer
        {
            public string Name { get; set; }

            public List<AttachmentDetails> Attachments { get; set; }
        }

        private class Class
        {
            public Customer Customer { get; set; }

            public List<Detail> Details { get; set; }
        }

        private class Detail
        {
            public long? Size { get; set; }
        }

        [Fact]
        public void CanPatch()
        {
            var stuff = new Stuff[3];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Numbers = new[] { 66 }, Stuff = stuff };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                var now = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers[0], 31);
                    session.Advanced.Patch<User, DateTime>(_docId, u => u.LastLogin, now);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers[0], 31);
                    Assert.Equal(loaded.LastLogin, now);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Stuff[0].Phone, "123456");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Stuff[0].Phone, "123456");
                }
            }
        }

        [Fact]
        public void CanPatchAndModify()
        {
            var user = new User { Numbers = new[] { 66 } };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    loaded.Numbers[0] = 1;
                    session.Advanced.Patch(loaded, u => u.Numbers[0], 2);
                    Assert.Throws<InvalidOperationException>(() =>
                    {
                        session.SaveChanges();
                    });
                }
            }
        }

        [Fact]
        public void CanPatchComplex()
        {
            var stuff = new Stuff[3];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Stuff = stuff };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<User, object>(_docId, u => u.Stuff[1],
                        new Stuff { Key = 4, Phone = "9255864406", Friend = new Friend() });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);

                    Assert.Equal(loaded.Stuff[1].Phone, "9255864406");
                    Assert.Equal(loaded.Stuff[1].Key, 4);
                    Assert.NotNull(loaded.Stuff[1].Friend);

                    session.Advanced.Patch(loaded, u => u.Stuff[2], new Stuff
                    {
                        Key = 4,
                        Phone = "9255864406",
                        Pet = new Pet { Name = "Hanan", Kind = "Dog" },
                        Friend = new Friend
                        {
                            Name = "Gonras",
                            Age = 28,
                            Pet = new Pet { Name = "Miriam", Kind = "Cat" }
                        },
                        Dic = new Dictionary<string, string>
                        {
                            {"Ohio", "Columbus"},
                            {"Utah", "Salt Lake City"},
                            {"Texas", "Austin"},
                            {"California", "Sacramento"},
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);

                    Assert.Equal(loaded.Stuff[2].Pet.Name, "Hanan");
                    Assert.Equal(loaded.Stuff[2].Friend.Name, "Gonras");
                    Assert.Equal(loaded.Stuff[2].Friend.Pet.Name, "Miriam");
                    Assert.Equal(loaded.Stuff[2].Dic.Count, 4);
                    Assert.Equal(loaded.Stuff[2].Dic["Utah"], "Salt Lake City");
                }
            }
        }

        [Fact]
        public void CanAddToArray()
        {
            var stuff = new Stuff[1];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Stuff = stuff, Numbers = new[] { 1, 2 } };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                WriteDocDirectlyFromStorageToTestOutput(store.Database, _docId);
                using (var session = store.OpenSession())
                {
                    //push
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(3));
                    session.Advanced.Patch<User, Stuff>(_docId, u => u.Stuff, roles => roles.Add(new Stuff { Key = 75 }));
                    session.SaveChanges();
                }
                WriteDocDirectlyFromStorageToTestOutput(store.Database, _docId);

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers[2], 3);
                    Assert.Equal(loaded.Stuff[1].Key, 75);

                    //concat
                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(101, 102, 103));
                    session.Advanced.Patch(loaded, u => u.Stuff, roles => roles.Add(new Stuff { Key = 102 }, new Stuff { Phone = "123456" }));
                    SaveChangesWithTryCatch(session, loaded);
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers.Length, 6);
                    Assert.Equal(loaded.Numbers[5], 103);

                    Assert.Equal(loaded.Stuff[2].Key, 102);
                    Assert.Equal(loaded.Stuff[3].Phone, "123456");

                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(new[] { 201, 202, 203 }));
                    SaveChangesWithTryCatch(session, loaded);
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers.Length, 9);
                    Assert.Equal(loaded.Numbers[7], 202);
                }
            }
        }

        [Fact]
        public void CanAddToArrayUsingParamsOverload()
        {
            var stuff = new Stuff[1];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Stuff = stuff, Numbers = new[] { 1, 2 } };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);

                    var items = new[]
                    {
                        new Stuff
                        {
                            Key = 102
                        },
                        new Stuff
                        {
                            Phone = "123456"
                        }
                    };

                    session.Advanced.Patch(loaded, u => u.Stuff, roles => roles.Add(items));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);

                    var numbers = new List<int> { 3, 4 };

                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(numbers.ToArray()));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);

                    Assert.Equal(loaded.Stuff[1].Key, 102);
                    Assert.Equal(loaded.Stuff[2].Phone, "123456");
                    Assert.Equal(loaded.Numbers[2], 3);
                    Assert.Equal(loaded.Numbers[3], 4);
                }
            }
        }

        [Fact]
        public void CanRemoveFromArray()
        {
            var stuff = new Stuff[2];
            stuff[0] = new Stuff { Key = 6 };
            stuff[1] = new Stuff { Phone = "123456" };
            var user = new User { Stuff = stuff, Numbers = new[] { 1, 2, 3 } };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.RemoveAt(1));
                    session.Advanced.Patch<User, object>(_docId, u => u.Stuff, roles => roles.RemoveAt(0));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);

                    Assert.Equal(loaded.Numbers.Length, 2);
                    Assert.Equal(loaded.Numbers[1], 3);

                    Assert.Equal(loaded.Stuff.Length, 1);
                    Assert.Equal(loaded.Stuff[0].Phone, "123456");
                }
            }
        }

        [Fact]
        public void CanIncrement()
        {
            Stuff[] s = new Stuff[3];
            s[0] = new Stuff { Key = 6 };
            var user = new User { Numbers = new[] { 66 }, Stuff = s };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Increment<User, int>(_docId, u => u.Numbers[0], 1);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers[0], 67);

                    // infer type & the id from entity
                    session.Advanced.Increment(loaded, u => u.Stuff[0].Key, -3);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Stuff[0].Key, 3);
                }
            }
        }

        [Fact]
        public void ShouldMergePatchCalls()
        {
            var stuff = new Stuff[3];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Numbers = new[] { 66 }, Stuff = stuff };
            var user2 = new User { Numbers = new[] { 1, 2, 3 }, Stuff = stuff };
            var docId2 = "users/2-A";


            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.Store(user2, docId2);
                    session.SaveChanges();
                }

                var now = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers[0], 31);
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, DateTime>(_docId, u => u.LastLogin, now);
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(docId2, u => u.Numbers[0], 123);
                    Assert.Equal(2, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, DateTime>(docId2, u => u.LastLogin, now);
                    Assert.Equal(2, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Increment<User, int>(_docId, u => u.Numbers[0], 1);
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(77));
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(88));
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.RemoveAt(1));
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void CanRemoveAllFromArray()
        {
            var customer = new Customer
            {
                Name = "Jerry",
                Attachments = new List<AttachmentDetails>
                {
                    new AttachmentDetails{ Name = "picture", Size = 12}, 
                    new AttachmentDetails{ Name = "picture", Size = 34 },
                    new AttachmentDetails{ Name = "file", Size = 56},
                    new AttachmentDetails{ Name = "file", Size = 78},
                    new AttachmentDetails{ Name = "file", Size = 99},
                    new AttachmentDetails{ Name = "video" , Size = 101}
                }
            };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(customer, "customers/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<Customer, AttachmentDetails>(
                        "customers/1", 
                        x => x.Attachments, 
                        x => x.RemoveAll(y => y.Name == "file"));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Customer>("customers/1");

                    Assert.Equal(3, loaded.Attachments.Count);

                    Assert.Equal("picture", loaded.Attachments[0].Name);
                    Assert.Equal(12, loaded.Attachments[0].Size);
                    Assert.Equal("picture", loaded.Attachments[1].Name);
                    Assert.Equal(34, loaded.Attachments[1].Size);
                    Assert.Equal("video", loaded.Attachments[2].Name);
                    Assert.Equal(101, loaded.Attachments[2].Size);

                    // infer type & the id from entity
                    session.Advanced.Patch(
                        loaded,
                        x => x.Attachments,
                        x => x.RemoveAll(y => y.Size < 30 || y.Size > 100));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Customer>("customers/1");

                    Assert.Equal(1, loaded.Attachments.Count);

                    Assert.Equal("picture", loaded.Attachments[0].Name);
                    Assert.Equal(34, loaded.Attachments[0].Size);

                }
            }
        }

        [Fact]
        public async Task CanPatchAsync()
        {
            var stuff = new Stuff[3];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Numbers = new[] { 66 }, Stuff = stuff };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var now = DateTime.Now;

                using (var session = store.OpenAsyncSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers[0], 31);
                    session.Advanced.Patch<User, DateTime>(_docId, u => u.LastLogin, now);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    Assert.Equal(loaded.Numbers[0], 31);
                    Assert.Equal(loaded.LastLogin, now);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Stuff[0].Phone, "123456");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    Assert.Equal(loaded.Stuff[0].Phone, "123456");
                }
            }
        }

        [Fact]
        public async Task CanPatchAndModifyAsync()
        {
            var user = new User { Numbers = new[] { 66 } };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    loaded.Numbers[0] = 1;
                    session.Advanced.Patch(loaded, u => u.Numbers[0], 2);
                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await session.SaveChangesAsync();
                    });
                }
            }
        }

        [Fact]
        public async Task CanPatchComplexAsync()
        {
            var stuff = new Stuff[3];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Stuff = stuff };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Patch<User, object>(_docId, u => u.Stuff[1],
                        new Stuff { Key = 4, Phone = "9255864406", Friend = new Friend() });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);

                    Assert.Equal(loaded.Stuff[1].Phone, "9255864406");
                    Assert.Equal(loaded.Stuff[1].Key, 4);
                    Assert.NotNull(loaded.Stuff[1].Friend);

                    session.Advanced.Patch(loaded, u => u.Stuff[2], new Stuff
                    {
                        Key = 4,
                        Phone = "9255864406",
                        Pet = new Pet { Name = "Hanan", Kind = "Dog" },
                        Friend = new Friend
                        {
                            Name = "Gonras",
                            Age = 28,
                            Pet = new Pet { Name = "Miriam", Kind = "Cat" }
                        },
                        Dic = new Dictionary<string, string>
                        {
                            {"Ohio", "Columbus"},
                            {"Utah", "Salt Lake City"},
                            {"Texas", "Austin"},
                            {"California", "Sacramento"},
                        }
                    });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);

                    Assert.Equal(loaded.Stuff[2].Pet.Name, "Hanan");
                    Assert.Equal(loaded.Stuff[2].Friend.Name, "Gonras");
                    Assert.Equal(loaded.Stuff[2].Friend.Pet.Name, "Miriam");
                    Assert.Equal(loaded.Stuff[2].Dic.Count, 4);
                    Assert.Equal(loaded.Stuff[2].Dic["Utah"], "Salt Lake City");
                }
            }
        }

        [Fact]
        public async Task CanAddToArrayAsync()
        {
            var stuff = new Stuff[1];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Stuff = stuff, Numbers = new[] { 1, 2 } };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }
                await WriteDocDirectlyFromStorageToTestOutputAsync(store.Database, _docId);

                using (var session = store.OpenAsyncSession())
                {
                    //push
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(3));
                    session.Advanced.Patch<User, Stuff>(_docId, u => u.Stuff, roles => roles.Add(new Stuff { Key = 75 }));
                    await session.SaveChangesAsync();
                }
                await WriteDocDirectlyFromStorageToTestOutputAsync(store.Database, _docId);

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    Assert.Equal(loaded.Numbers[2], 3);
                    Assert.Equal(loaded.Stuff[1].Key, 75);

                    //concat
                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(101, 102, 103));
                    session.Advanced.Patch(loaded, u => u.Stuff, roles => roles.Add(new Stuff { Key = 102 }, new Stuff { Phone = "123456" }));
                    await SaveChangesWithTryCatchAsync(session, loaded);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    Assert.Equal(loaded.Numbers.Length, 6);
                    Assert.Equal(loaded.Numbers[5], 103);

                    Assert.Equal(loaded.Stuff[2].Key, 102);
                    Assert.Equal(loaded.Stuff[3].Phone, "123456");

                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(new[] { 201, 202, 203 }));
                    await SaveChangesWithTryCatchAsync(session, loaded);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    Assert.Equal(loaded.Numbers.Length, 9);
                    Assert.Equal(loaded.Numbers[7], 202);
                }
            }
        }

        [Fact]
        public async Task CanRemoveFromArrayAsync()
        {
            var stuff = new Stuff[2];
            stuff[0] = new Stuff { Key = 6 };
            stuff[1] = new Stuff { Phone = "123456" };
            var user = new User { Stuff = stuff, Numbers = new[] { 1, 2, 3 } };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.RemoveAt(1));
                    session.Advanced.Patch<User, object>(_docId, u => u.Stuff, roles => roles.RemoveAt(0));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);

                    Assert.Equal(loaded.Numbers.Length, 2);
                    Assert.Equal(loaded.Numbers[1], 3);

                    Assert.Equal(loaded.Stuff.Length, 1);
                    Assert.Equal(loaded.Stuff[0].Phone, "123456");
                }
            }
        }

        [Fact]
        public async Task CanIncrementAsync()
        {
            Stuff[] s = new Stuff[3];
            s[0] = new Stuff { Key = 6 };
            var user = new User { Numbers = new[] { 66 }, Stuff = s };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                await WriteDocDirectlyFromStorageToTestOutputAsync(store.Database, _docId);

                using (var session = store.OpenAsyncSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Increment<User, int>(_docId, u => u.Numbers[0], 1);
                    await session.SaveChangesAsync();
                }
                await WriteDocDirectlyFromStorageToTestOutputAsync(store.Database, _docId);

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    Assert.Equal(loaded.Numbers[0], 67);

                    // infer type & the id from entity
                    session.Advanced.Increment(loaded, u => u.Stuff[0].Key, -3);
                    await SaveChangesWithTryCatchAsync(session, loaded);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    Assert.Equal(loaded.Stuff[0].Key, 3);
                }
            }
        }

        [Fact]
        public async Task ShouldMergePatchCallsAsync()
        {
            var stuff = new Stuff[3];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Numbers = new[] { 66 }, Stuff = stuff };
            var user2 = new User { Numbers = new[] { 1, 2, 3 }, Stuff = stuff };
            var docId2 = "users/2-A";


            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.StoreAsync(user2, docId2);
                    await session.SaveChangesAsync();
                }

                var now = DateTime.Now;

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers[0], 31);
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, DateTime>(_docId, u => u.LastLogin, now);
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(docId2, u => u.Numbers[0], 123);
                    Assert.Equal(2, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, DateTime>(docId2, u => u.LastLogin, now);
                    Assert.Equal(2, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Increment<User, int>(_docId, u => u.Numbers[0], 1);
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(77));
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(88));
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.RemoveAt(1));
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task CanRemoveAllFromArrayAsync()
        {
            var customer = new Customer
            {
                Name = "Jerry",
                Attachments = new List<AttachmentDetails>
                {
                    new AttachmentDetails{ Name = "picture", Size = 12},
                    new AttachmentDetails{ Name = "picture", Size = 34 },
                    new AttachmentDetails{ Name = "file", Size = 56},
                    new AttachmentDetails{ Name = "file", Size = 78},
                    new AttachmentDetails{ Name = "file", Size = 99},
                    new AttachmentDetails{ Name = "video" , Size = 101}
                }
            };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(customer, "customers/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<Customer, AttachmentDetails>(
                        "customers/1",
                        x => x.Attachments,
                        x => x.RemoveAll(y => y.Name == "file"));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<Customer>("customers/1");

                    Assert.Equal(3, loaded.Attachments.Count);

                    Assert.Equal("picture", loaded.Attachments[0].Name);
                    Assert.Equal(12, loaded.Attachments[0].Size);
                    Assert.Equal("picture", loaded.Attachments[1].Name);
                    Assert.Equal(34, loaded.Attachments[1].Size);
                    Assert.Equal("video", loaded.Attachments[2].Name);
                    Assert.Equal(101, loaded.Attachments[2].Size);

                    // infer type & the id from entity
                    session.Advanced.Patch(
                        loaded,
                        x => x.Attachments,
                        x => x.RemoveAll(y => y.Size < 30 || y.Size > 100));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<Customer>("customers/1");

                    Assert.Equal(1, loaded.Attachments.Count);

                    Assert.Equal("picture", loaded.Attachments[0].Name);
                    Assert.Equal(34, loaded.Attachments[0].Size);
                }
            }
        }

        [Fact]
        public void PatchNullField_ExpectFieldSetToNull()
        {
            using (var store = GetDocumentStore())
            {
                string entityId;
                using (var session = store.OpenSession())
                {
                    var user = new Order
                    {
                        ShipTo = new Address
                        {
                            Street = Guid.NewGuid().ToString()
                        }
                    };
                    session.Store(user);
                    session.SaveChanges();
                    entityId = user.Id;
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Query<Order>().First();
                    Assert.NotNull(order.ShipTo);
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Order, Address>(entityId, x => x.ShipTo, null);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Query<Order>().First();
                    Assert.Null(order.ShipTo);
                }
            }
        }

        [Fact]
        public void CanUseLinq()
        {
            using (var store = GetDocumentStore())
            {
                const string changeVector = "ravendb-logo.png";
                const string id = "doc";
                const int newSize = 2;

                using (var session = store.OpenSession())
                {
                    session.Store(new Class
                    {
                        Customer = new Customer
                        {
                            Attachments = new List<AttachmentDetails>
                            {
                                new AttachmentDetails
                                {
                                    ChangeVector = changeVector,
                                    Size = 1
                                }
                            }
                        }
                    }, id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Class, long>(id, x => x.Customer.Attachments.Where(q => q.ChangeVector == changeVector).FirstOrDefault().Size, newSize);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Class>(id);
                    var attachmentDetails = doc.Customer.Attachments.FirstOrDefault(q => q.ChangeVector == changeVector);
                    Assert.NotNull(attachmentDetails);
                    Assert.Equal(newSize, attachmentDetails.Size);
                }
            }
        }

        [Fact]
        public void CanUseNullableType()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "doc";
                const int newSize = 2;

                using (var session = store.OpenSession())
                {
                    session.Store(new Class
                    {
                        Details = new List<Detail>
                        {
                            new Detail
                            {
                                Size = 0
                            }
                        }
                    }, id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Class, long?>(id, x => x.Details.Where(q => q.Size.HasValue).FirstOrDefault().Size, newSize);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Class>(id);
                    var details = doc.Details.FirstOrDefault();
                    Assert.NotNull(details);
                    Assert.Equal(newSize, details.Size);
                }
            }
        }
    }
}
