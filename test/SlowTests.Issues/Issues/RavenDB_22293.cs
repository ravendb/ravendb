using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Tests.Infrastructure;
using Xunit;

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

        [RavenTheory(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        [InlineData(SessionPatchBehavior.JsonPatch, CommandType.JsonPatch)]
        [InlineData(SessionPatchBehavior.JavaScript, CommandType.PATCH)]
        public void Patch_SimpleProperty_UsesConfiguredBehavior(SessionPatchBehavior behavior, CommandType expected)
        {
            using (var store = GetPatchStore(behavior))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Original", Age = 25 }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Name, "Updated");
                    AssertUsesPatchBehavior(session, "users/1", expected);
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

        [RavenTheory(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        [InlineData(SessionPatchBehavior.JsonPatch, CommandType.JsonPatch)]
        [InlineData(SessionPatchBehavior.JavaScript, CommandType.PATCH)]
        public void Patch_ArrayAdd_UsesConfiguredBehavior(SessionPatchBehavior behavior, CommandType expected)
        {
            using (var store = GetPatchStore(behavior))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Tags = new List<string> { "a", "b" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Tags, tags => tags.Add("c"));
                    AssertUsesPatchBehavior(session, "users/1", expected);
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

        [RavenTheory(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        [InlineData(SessionPatchBehavior.JsonPatch, CommandType.JsonPatch)]
        [InlineData(SessionPatchBehavior.JavaScript, CommandType.PATCH)]
        public void Patch_ArrayRemoveAt_UsesConfiguredBehavior(SessionPatchBehavior behavior, CommandType expected)
        {
            using (var store = GetPatchStore(behavior))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Tags = new List<string> { "a", "b", "c" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Tags, tags => tags.RemoveAt(1));
                    AssertUsesPatchBehavior(session, "users/1", expected);
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

        [RavenTheory(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        [InlineData(SessionPatchBehavior.JsonPatch, CommandType.JsonPatch)]
        [InlineData(SessionPatchBehavior.JavaScript, CommandType.PATCH)]
        public void Patch_DictionaryAdd_UsesConfiguredBehavior(SessionPatchBehavior behavior, CommandType expected)
        {
            using (var store = GetPatchStore(behavior))
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
                    session.Advanced.Patch<UserWithTags, string, string>("users/1", u => u.Settings, d => d.Add("lang", "en"));
                    AssertUsesPatchBehavior(session, "users/1", expected);
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

        [RavenTheory(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        [InlineData(SessionPatchBehavior.JsonPatch, CommandType.JsonPatch)]
        [InlineData(SessionPatchBehavior.JavaScript, CommandType.PATCH)]
        public void Patch_DictionaryRemove_UsesConfiguredBehavior(SessionPatchBehavior behavior, CommandType expected)
        {
            using (var store = GetPatchStore(behavior))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags
                    {
                        Name = "Test",
                        Settings = new Dictionary<string, string> { { "theme", "light" }, { "lang", "en" } }
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string, string>("users/1", u => u.Settings, d => d.Remove("lang"));
                    AssertUsesPatchBehavior(session, "users/1", expected);
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
                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch);
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
                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch);
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
                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch);
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

                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch);
                    AssertDeferredCommandCount(session, 1);

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

                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch);

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
                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch);
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

                    AssertDeferredCommand(session, "users/1", CommandType.PATCH);
                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch, exists: false);

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

                    AssertDeferredCommand(session, "users/1", CommandType.PATCH);
                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch, exists: false);

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

                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch);
                    AssertDeferredCommand(session, "users/1", CommandType.PATCH);
                    AssertDeferredCommandCount(session, 2);

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

                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch);

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
                    session.Advanced.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes;

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
                    session.Advanced.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes;

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

                    AssertDeferredCommand(session, "users/1", CommandType.PATCH);
                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch, exists: false);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(15, user.Age);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_AbsentProperty_CreatesPropertyViaJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                // Store a document that does NOT contain the 'Age' property at all
                // (simulating schema evolution / a document written under an older shape,
                // or a store configured to omit nulls). A JsonPatch "replace" would throw
                // server-side; the old JavaScript "this.Age = value" silently created it.
                using (var commands = store.Commands())
                {
                    commands.Put("users/1", null, new { Name = "Test" },
                        new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "UserWithTags" } });
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, int>("users/1", u => u.Age, 30);
                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch);
                    session.SaveChanges(); // must not throw even though 'Age' is absent on the stored document
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal("Test", user.Name);
                    Assert.Equal(30, user.Age);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_ArrayElementByIndex_ReplacesInPlace()
        {
            // Guards the JsonPatch op selection: assigning to an existing positional element
            // must overwrite it ("replace"), not insert before it ("add").
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Tags = new List<string> { "a", "b", "c" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Tags[1], "B");
                    AssertDeferredCommand(session, "users/1", CommandType.JsonPatch);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(3, user.Tags.Count); // replaced in place, not inserted
                    Assert.Equal("a", user.Tags[0]);
                    Assert.Equal("B", user.Tags[1]);
                    Assert.Equal("c", user.Tags[2]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void Patch_Value_UsesStoreSerializationConventions()
        {
            // A value routed through JsonPatch must be serialized with the store's configured
            // conventions (custom converter -> "30C"), not DocumentConventions.Default
            // (which would emit an object like {"Celsius":30}).
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer => serializer.Converters.Add(new TemperatureConverter())
                    };
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new WeatherDoc { Temp = new Temperature { Celsius = 20 } }, "weather/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<WeatherDoc, Temperature>("weather/1", x => x.Temp, new Temperature { Celsius = 30 });
                    AssertDeferredCommand(session, "weather/1", CommandType.JsonPatch);
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var doc = commands.Get("weather/1").BlittableJson;
                    Assert.True(doc.TryGet(nameof(WeatherDoc.Temp), out string temp),
                        "Temp should be serialized as a string via the store's custom converter");
                    Assert.Equal("30C", temp);
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<WeatherDoc>("weather/1");
                    Assert.Equal(30, doc.Temp.Celsius);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void SessionPatchBehavior_DefaultsToJsonPatch()
        {
            using (var store = GetDocumentStore())
            {
                Assert.Equal(SessionPatchBehavior.JsonPatch, store.Conventions.SessionPatchBehavior);
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void SessionPatchBehavior_ArrayRemoveAtOutOfRange_ThrowsUnderJsonPatch_NoOpUnderJavaScript()
        {
            // Default (JsonPatch): out-of-range RemoveAt reaches the server as `remove /Tags/10`,
            // which strict RFC 6902 remove rejects -> SaveChanges throws.
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Tags = new List<string> { "a", "b", "c" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Tags, tags => tags.RemoveAt(10));
                    Assert.ThrowsAny<RavenException>(() => session.SaveChanges());
                }
            }

            // JavaScript convention mitigates it: legacy splice(10, 1) is a silent no-op.
            using (var store = GetPatchStore(SessionPatchBehavior.JavaScript))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserWithTags { Name = "Test", Tags = new List<string> { "a", "b", "c" } }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<UserWithTags, string>("users/1", u => u.Tags, tags => tags.RemoveAt(10));
                    session.SaveChanges(); // no-op, must not throw
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Equal(3, user.Tags.Count); // unchanged
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
        public void SessionPatchBehavior_DictionaryRemoveAbsentKey_ThrowsUnderJsonPatch_NoOpUnderJavaScript()
        {
            // Default (JsonPatch): removing a missing key reaches the server as `remove /Settings/missing`,
            // which strict RFC 6902 remove rejects -> SaveChanges throws.
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
                    session.Advanced.Patch<UserWithTags, string, string>("users/1", u => u.Settings, d => d.Remove("missing"));
                    Assert.ThrowsAny<RavenException>(() => session.SaveChanges());
                }
            }

            // JavaScript convention mitigates it: `delete obj['missing']` is a silent no-op.
            using (var store = GetPatchStore(SessionPatchBehavior.JavaScript))
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
                    session.Advanced.Patch<UserWithTags, string, string>("users/1", u => u.Settings, d => d.Remove("missing"));
                    session.SaveChanges(); // no-op, must not throw
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserWithTags>("users/1");
                    Assert.Single(user.Settings);
                    Assert.Equal("light", user.Settings["theme"]);
                }
            }
        }

        private IDocumentStore GetPatchStore(SessionPatchBehavior behavior)
        {
            return GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.SessionPatchBehavior = behavior
            });
        }

        private static void AssertUsesPatchBehavior(IDocumentSession session, string id, CommandType expected)
        {
            var other = expected == CommandType.JsonPatch ? CommandType.PATCH : CommandType.JsonPatch;
            AssertDeferredCommand(session, id, expected);
            AssertDeferredCommand(session, id, other, exists: false);
        }

        private static void AssertDeferredCommand(IDocumentSession session, string id, CommandType type, bool exists = true)
        {
            var sessionOps = (InMemoryDocumentSessionOperations)session;
            Assert.Equal(exists, sessionOps.DeferredCommandsDictionary.ContainsKey((id, type, null)));
        }

        private static void AssertDeferredCommandCount(IDocumentSession session, int count)
        {
            var sessionOps = (InMemoryDocumentSessionOperations)session;
            Assert.Equal(count, sessionOps.DeferredCommandsCount);
        }

        private struct Temperature
        {
            public int Celsius { get; set; }
        }

        private class TemperatureConverter : JsonConverter
        {
            public override bool CanConvert(Type objectType) => objectType == typeof(Temperature);

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                writer.WriteValue($"{((Temperature)value).Celsius}C");
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var s = (string)reader.Value;
                return new Temperature { Celsius = int.Parse(s.Substring(0, s.Length - 1)) };
            }
        }

        private class WeatherDoc
        {
            public string Id { get; set; }
            public Temperature Temp { get; set; }
        }
    }
}
