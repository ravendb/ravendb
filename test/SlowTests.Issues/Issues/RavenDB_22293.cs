using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22293 : RavenTestBase
    {
        public RavenDB_22293(ITestOutputHelper output) : base(output)
        {
        }

        private class UserWithTags
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public List<string> Tags { get; set; }
            public Dictionary<string, string> Settings { get; set; }
            public Address Address { get; set; }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_SimpleProperty_UsesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Original", Age = 25 }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Name, "Updated");

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal("Updated", user.Name);
                    Assert.Equal(25, user.Age);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_NestedProperty_UsesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags
                    {
                        Name = "Test",
                        Address = new Address { City = "OldCity", Country = "US" }
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Address.City, "NewCity");

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal("NewCity", user.Address.City);
                    Assert.Equal("US", user.Address.Country);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_NullValue_UsesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Name, null);

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Null(user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_IntegerValue_UsesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Age = 25 }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, int>("users/1", u => u.Age, 30);

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(30, user.Age);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_MultipleProperties_MergesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Original", Age = 25 }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Name, "Updated");
                    session.Advanced.Patch<UserWithTags, int>("users/1", u => u.Age, 30);

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));
                    Assert.Equal(1, sessionOps.DeferredCommandsCount);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal("Updated", user.Name);
                    Assert.Equal(30, user.Age);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_TrackedEntity_UpdatesAfterSaveChanges()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Original" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    session.Advanced.Patch(user, u => u.Name, "Updated");

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    var cv = session.Advanced.GetChangeVectorFor(user);

                    session.SaveChanges();

                    Assert.Equal("Updated", user.Name);
                    Assert.NotEqual(cv, session.Advanced.GetChangeVectorFor(user));

                    user.Age = 30;
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal("Updated", user.Name);
                    Assert.Equal(30, user.Age);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_ArrayAdd_UsesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Tags = new List<string> { "a", "b" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Tags, tags => tags.Add("c"));

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(3, user.Tags.Count);
                    Assert.Equal("c", user.Tags[2]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_ArrayAddMultiple_UsesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Tags = new List<string> { "a" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Tags, tags => tags.Add("b", "c"));

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(3, user.Tags.Count);
                    Assert.Equal("b", user.Tags[1]);
                    Assert.Equal("c", user.Tags[2]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_ArrayRemoveAt_UsesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Tags = new List<string> { "a", "b", "c" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Tags, tags => tags.RemoveAt(1));

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(2, user.Tags.Count);
                    Assert.Equal("a", user.Tags[0]);
                    Assert.Equal("c", user.Tags[1]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_ArrayRemoveAll_FallsBackToJavaScript()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Tags = new List<string> { "a", "b", "c" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Tags, tags => tags.RemoveAll(t => t == "b"));

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.PATCH, null)));
                    Assert.False(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(2, user.Tags.Count);
                    Assert.Contains("a", user.Tags);
                    Assert.Contains("c", user.Tags);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_DictionaryAdd_UsesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags
                    {
                        Name = "Test",
                        Settings = new Dictionary<string, string> { { "theme", "light" } }
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string, string>("users/1",
                        u => u.Settings,
                        d => d.Add("lang", "en"));

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(2, user.Settings.Count);
                    Assert.Equal("light", user.Settings["theme"]);
                    Assert.Equal("en", user.Settings["lang"]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_DictionaryRemove_UsesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags
                    {
                        Name = "Test",
                        Settings = new Dictionary<string, string>
                        {
                            { "theme", "light" },
                            { "lang", "en" }
                        }
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string, string>("users/1",
                        u => u.Settings,
                        d => d.Remove("lang"));

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Single(user.Settings);
                    Assert.Equal("light", user.Settings["theme"]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_AfterIncrement_FallsBackToJavaScript()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Original", Age = 25 }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // Increment creates a JavaScript patch
                    session.Advanced.Increment<UserWithTags, int>("users/1", u => u.Age, 5);
                    // Patch should fall back to JavaScript and merge
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Name, "Updated");

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.PATCH, null)));
                    Assert.False(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal("Updated", user.Name);
                    Assert.Equal(30, user.Age);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_ThenIncrement_KeepsBothCommands()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Original", Age = 25 }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // Patch creates a JsonPatch command
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Name, "Updated");
                    // Increment creates a separate JavaScript patch command
                    session.Advanced.Increment<UserWithTags, int>("users/1", u => u.Age, 5);

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.PATCH, null)));
                    Assert.Equal(2, sessionOps.DeferredCommandsCount);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal("Updated", user.Name);
                    Assert.Equal(30, user.Age);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_EntityOverload_UsesJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Original" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    session.Advanced.Patch(user, u => u.Name, "Updated");

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();

                    Assert.Equal("Updated", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public async Task Patch_WithOptimisticConcurrency_DoesNotSendChangeVector_JsonPatch()
        {
            // Session.Advanced.Patch does not enforce optimistic concurrency (change vector is always null).
            // A concurrent modification between Load and Patch+SaveChanges should succeed, not throw ConcurrencyException.
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Original", Age = 25 }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    var user = await session.LoadAsync<UserWithTags>("users/1");

                    // Simulate a concurrent modification by another session
                    using (var otherSession = store.OpenSession())
                    {
                        var otherUser = otherSession.Load<UserWithTags>("users/1");
                        otherUser.Age = 99;
                        otherSession.SaveChanges();
                    }

                    // JsonPatch path
                    session.Advanced.Patch(user, u => u.Name, "Updated");

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    await session.SaveChangesAsync(); // should not throw ConcurrencyException
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal("Updated", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public async Task Patch_WithOptimisticConcurrency_DoesNotSendChangeVector_JavaScript()
        {
            // Same test as above but forcing the JavaScript patch path via Increment,
            // to confirm both paths behave identically with optimistic concurrency.
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Original", Age = 25 }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    var user = await session.LoadAsync<UserWithTags>("users/1");

                    // Simulate a concurrent modification by another session
                    using (var otherSession = store.OpenSession())
                    {
                        var otherUser = otherSession.Load<UserWithTags>("users/1");
                        otherUser.Age = 99;
                        otherSession.SaveChanges();
                    }

                    // JavaScript patch path (Increment always uses JavaScript)
                    session.Advanced.Increment<UserWithTags, int>(user, u => u.Age, 1);

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.PATCH, null)));

                    await session.SaveChangesAsync(); // should not throw ConcurrencyException
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(100, user.Age); // 99 + 1
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Increment_StaysAsJavaScript()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Age = 10 }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Increment<UserWithTags, int>("users/1", u => u.Age, 5);

                    var sessionOps = (InMemoryDocumentSessionOperations)session;
                    Assert.True(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.PATCH, null)));
                    Assert.False(sessionOps.DeferredCommandsDictionary.ContainsKey(("users/1", CommandType.JsonPatch, null)));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(15, user.Age);
                }
            }
        }
    }
}
