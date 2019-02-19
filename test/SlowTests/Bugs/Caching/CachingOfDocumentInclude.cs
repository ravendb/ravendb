//-----------------------------------------------------------------------
// <copyright file="CachingOfDocumentInclude.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;

namespace SlowTests.Bugs.Caching
{
    public class CachingOfDocumentInclude : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }

        [Fact]
        public void Can_cache_document_with_includes()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1-A" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2-A");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2-A");

                    Assert.Equal(1, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                }
            }
        }

        [Fact]
        public async Task Can_avoid_using_server_for_load_with_include_if_everything_is_in_session_cacheAsync()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1-A" });
                    s.SaveChanges();
                }

                using (var s = store.OpenAsyncSession())
                {
                    var u = await s.LoadAsync<User>("users/2-A");

                    await s.LoadAsync<User>(u.PartnerId);

                    var old = s.Advanced.NumberOfRequests;
                    var res = await s.Include<User>(x => x.PartnerId)
                         .LoadAsync("users/2-A");

                    Assert.Equal(old, s.Advanced.NumberOfRequests);
                }
            }
        }
        [Fact]
        public void Can_avoid_using_server_for_load_with_include_if_everything_is_in_session_cacheLazy()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1-A" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Advanced.Lazily.Load<User>("users/2-A");
                    s.Advanced.Lazily.Load<User>("users/1-A");
                    s.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                    var old = s.Advanced.NumberOfRequests;
                    Lazy<User> result1 = s.Advanced.Lazily
                        .Include<User>(x => x.PartnerId)
                        .Load<User>("users/2-A");
                    Assert.NotNull(result1.Value);
                    Assert.Equal(old, s.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_avoid_using_server_for_load_with_include_if_everything_is_in_session_cache()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1-A" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/2-A");

                    s.Load<User>(u.PartnerId);

                    var old = s.Advanced.NumberOfRequests;
                    var res = s.Include<User>(x => x.PartnerId)
                         .Load("users/2-A");

                    Assert.Equal(old, s.Advanced.NumberOfRequests);
                }
            }
        }
        [Fact]
        public void Can_avoid_using_server_for_multiload_with_include_if_everything_is_in_session_cache()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Additional" });
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Michael" });
                    s.Store(new User { Name = "Fitzhak" });
                    s.Store(new User { Name = "Maxim" });
                    s.Store(new User { PartnerId = "users/1-A" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var u2 = s.Load<User>("users/2-A");
                    var u6 = s.Load<User>("users/6-A");
                    var inp = new List<string>();
                    inp.Add("users/1-A");
                    inp.Add("users/2-A");
                    inp.Add("users/3-A");
                    inp.Add("users/4-A");
                    inp.Add("users/5-A");
                    inp.Add("users/6-A");
                    var u4 = s.Load<User>(inp.ToArray());

                    s.Load<User>(u6.PartnerId);

                    var old = s.Advanced.NumberOfRequests;
                    var res = s.Include<User>(x => x.PartnerId)
                         .Load("users/2-A", "users/3-A", "users/6-A");

                    Assert.Equal(old, s.Advanced.NumberOfRequests);
                }
            }
        }
        [Fact]
        public void Will_refresh_result_when_main_document_changes()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1-A" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2-A");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var user = s.Include<User>(x => x.PartnerId)
                        .Load("users/2-A");
                    Assert.Equal(1, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                    user.Name = "Foo";
                    s.SaveChanges();
                }


                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2-A");
                    Assert.Equal(1, s.Advanced.RequestExecutor.Cache.NumberOfItems); // did NOT increase cache
                }
            }
        }

        [Fact]
        public void New_query_returns_correct_value_when_cache_is_enabled_and_data_changes()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende", Email = "same.email@example.com" });

                    store.Maintenance.Send(new PutIndexesOperation(new[] {
                        new IndexDefinition
                        {
                            Maps = { "from user in docs.Users select new {Email=user.Email}" },
                            Name = "index"
                        }}));

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Query<User>("index")
                        .Customize(q => q.WaitForNonStaleResults())
                        .Where(u => u.Email == "same.email@example.com")
                        .ToArray();
                    // Cache is done by url, so including a cutoff date invalidates the cache.

                    // the second query should stay in cache and return the correct value
                    results = s.Query<User>("index")
                        .Where(u => u.Email == "same.email@example.com")
                        .ToArray();
                    Assert.Equal(1, results.Length);

                    //to ensure that latestEtag2 != latestEtag1
                    s.Store(new object());
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Other", Email = "same.email@example.com" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Query<User>("index")
                        .Customize(q => q.WaitForNonStaleResults())
                        .Where(u => u.Email == "same.email@example.com")
                        .ToArray();
                    // this works, since we don't hit the cache
                    Assert.Equal(2, results.Length);

                    // we now hit the cache, but it should be invalidated since the underlying index *has* changed
                    // it isn't invalidated, and the result returns just 1 result
                    results = s.Query<User>("index")
                        .Where(u => u.Email == "same.email@example.com")
                        .ToArray();
                    Assert.Equal(2, results.Length);
                }
            }
        }

        [Fact]
        public void Will_refresh_result_when_included_document_changes()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1-A" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2-A");
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2-A");
                    Assert.Equal(1, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                    s.Load<User>("users/1-A").Name = "foo";
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2-A");
                    Assert.Equal(1, s.Advanced.RequestExecutor.Cache.NumberOfItems); // did NOT increase cache
                }
            }
        }
    }
}
