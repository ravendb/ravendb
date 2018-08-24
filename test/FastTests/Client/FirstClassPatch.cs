using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Xunit;

namespace FastTests.Client
{
    public class FirstClassPatch : RavenTestBase
    {
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

                using (var session = store.OpenSession())
                {
                    //push
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(3));
                    session.Advanced.Patch<User, Stuff>(_docId, u => u.Stuff, roles => roles.Add(new Stuff { Key = 75 }));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers[2], 3);
                    Assert.Equal(loaded.Stuff[1].Key, 75);

                    //concat
                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(101, 102, 103));
                    session.Advanced.Patch(loaded, u => u.Stuff, roles => roles.Add(new Stuff { Key = 102 }, new Stuff { Phone = "123456" }));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers.Length, 6);
                    Assert.Equal(loaded.Numbers[5], 103);

                    Assert.Equal(loaded.Stuff[2].Key, 102);
                    Assert.Equal(loaded.Stuff[3].Phone, "123456");

                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(new[] { 201, 202, 203 }));
                    session.SaveChanges();
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

                    var numbers = new List<int> {3, 4};

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

                using (var session = store.OpenAsyncSession())
                {
                    //push
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(3));
                    session.Advanced.Patch<User, Stuff>(_docId, u => u.Stuff, roles => roles.Add(new Stuff { Key = 75 }));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    Assert.Equal(loaded.Numbers[2], 3);
                    Assert.Equal(loaded.Stuff[1].Key, 75);

                    //concat
                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(101, 102, 103));
                    session.Advanced.Patch(loaded, u => u.Stuff, roles => roles.Add(new Stuff { Key = 102 }, new Stuff { Phone = "123456" }));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    Assert.Equal(loaded.Numbers.Length, 6);
                    Assert.Equal(loaded.Numbers[5], 103);

                    Assert.Equal(loaded.Stuff[2].Key, 102);
                    Assert.Equal(loaded.Stuff[3].Phone, "123456");

                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(new[] { 201, 202, 203 }));
                    await session.SaveChangesAsync();
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

                using (var session = store.OpenAsyncSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Increment<User, int>(_docId, u => u.Numbers[0], 1);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>(_docId);
                    Assert.Equal(loaded.Numbers[0], 67);

                    // infer type & the id from entity
                    session.Advanced.Increment(loaded, u => u.Stuff[0].Key, -3);
                    await session.SaveChangesAsync();
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
    }
}
