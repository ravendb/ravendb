//-----------------------------------------------------------------------
// <copyright file="CachingOfDocumentInclude.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions;
using Xunit;

namespace NewClientTests.NewClient.Raven.Tests.Bugs.Caching
{
    public class CachingOfDocumentInclude : RavenTestBase
    {
        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }

            public bool Active { get; set; }
        }

        [Fact(Skip = "TODO : NumberOfCachedRequests Not Implemented")]
        public void Can_cache_document_with_includes()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenNewSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1" });
                    s.SaveChanges();
                }

                using (var s = store.OpenNewSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    s.SaveChanges();
                }

                using (var s = store.OpenNewSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
                }
            }
        }

        [Fact(Skip = "NotImplementedException")]
        public async Task Can_avoid_using_server_for_load_with_include_if_everything_is_in_session_cacheAsync()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenNewSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1" });
                    s.SaveChanges();
                }

                using (var s = store.OpenNewAsyncSession())
                {
                    var u = await s.LoadAsync<User>("users/2");

                    await s.LoadAsync<User>(u.PartnerId);

                    var old = s.Advanced.NumberOfRequests;
                    var res = await s.Include<User>(x => x.PartnerId)
                         .LoadAsync("users/2");

                    Assert.Equal(old, s.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact(Skip = "TODO : Lazy Not Implemented")]
        public void Can_avoid_using_server_for_load_with_include_if_everything_is_in_session_cacheLazy()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenNewSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1" });
                    s.SaveChanges();
                }

                using (var s = store.OpenNewSession())
                {
                    s.Advanced.Lazily.Load<User>("users/2");
                    s.Advanced.Lazily.Load<User>("users/1");
                    s.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                    var old = s.Advanced.NumberOfRequests;
                    Lazy<User> result1 = s.Advanced.Lazily
                        .Include<User>(x => x.PartnerId)
                        .Load<User>("users/2");
                    Assert.NotNull(result1.Value);
                    Assert.Equal(old, s.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact(Skip = "NotImplementedException")]
        public void Can_avoid_using_server_for_load_with_include_if_everything_is_in_session_cache()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenNewSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1" });
                    s.SaveChanges();
                }

                using (var s = store.OpenNewSession())
                {
                    var u = s.Load<User>("users/2");

                    s.Load<User>(u.PartnerId);

                    var old = s.Advanced.NumberOfRequests;
                    var res = s.Include<User>(x => x.PartnerId)
                         .Load("users/2");

                    Assert.Equal(old, s.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact(Skip = "NotImplementedException")]
        public void Can_avoid_using_server_for_multiload_with_include_if_everything_is_in_session_cache()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenNewSession())
                {
                    s.Store(new User { Name = "Additional" });
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Michael" });
                    s.Store(new User { Name = "Fitzhak" });
                    s.Store(new User { Name = "Maxim" });
                    s.Store(new User { PartnerId = "users/1" });
                    s.SaveChanges();
                }

                using (var s = store.OpenNewSession())
                {
                    var u2 = s.Load<User>("users/2");
                    var u6 = s.Load<User>("users/6");
                    var inp = new List<string>();
                    inp.Add("users/1");
                    inp.Add("users/2");
                    inp.Add("users/3");
                    inp.Add("users/4");
                    inp.Add("users/5");
                    inp.Add("users/6");
                    var u4 = s.Load<User>(inp.ToArray());

                    s.Load<User>(u6.PartnerId);

                    var old = s.Advanced.NumberOfRequests;
                    var res = s.Include<User>(x => x.PartnerId)
                         .Load("users/2", "users/3", "users/6");

                    Assert.Equal(old, s.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact(Skip = "TODO : NumberOfCachedRequests Not Implemented")]
        public void Will_refresh_result_when_main_document_changes()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenNewSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1" });
                    s.SaveChanges();
                }

                using (var s = store.OpenNewSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    s.SaveChanges();
                }

                using (var s = store.OpenNewSession())
                {
                    var user = s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
                    user.Name = "Foo";
                    s.SaveChanges();
                }


                using (var s = store.OpenNewSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests); // did NOT increase cache
                }
            }
        }

        [Fact(Skip = "TODO : DatabaseCommands.PutIndex Not Implemented")]
        public void New_query_returns_correct_value_when_cache_is_enabled_and_data_changes()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenNewSession())
                {
                    s.Store(new User { Name = "Ayende", Email = "same.email@example.com" });
                   /* store.DatabaseCommands.PutIndex("index",
                                                         new IndexDefinition()
                                                         {
                                                             Map =
                                                                     "from user in docs.Users select new {Email=user.Email}"
                                                         });*/
                    s.SaveChanges();
                }

                DateTime firstTime = SystemTime.UtcNow;

                using (var s = store.OpenNewSession())
                {
                   /* var results = s.Query<User>("index")
                        .Customize(q => q.WaitForNonStaleResultsAsOf(firstTime))
                        .Where(u => u.Email == "same.email@example.com")
                        .ToArray();
                    // Cache is done by url, so including a cutoff date invalidates the cache.

                    // the second query should stay in cache and return the correct value
                    results = s.Query<User>("index")
                        .Where(u => u.Email == "same.email@example.com")
                        .ToArray();
                    Assert.Equal(1, results.Length);*/
                }

                DateTime secondTime = SystemTime.UtcNow;

                if (firstTime == secondTime) // avoid getting the exact same url
                    secondTime = secondTime.AddMilliseconds(100);

                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Other", Email = "same.email@example.com" });
                    s.SaveChanges();
                }


                using (var s = store.OpenNewSession())
                {
                    /*var results = s.Query<User>("index")
                        .Customize(q => q.WaitForNonStaleResultsAsOf(secondTime))
                        .Where(u => u.Email == "same.email@example.com")
                        .ToArray();
                    // this works, since we don't hit the cache
                    Assert.Equal(2, results.Length);

                    // we now hit the cache, but it should be invalidated since the underlying index *has* changed
                    // it isn't invalidated, and the result returns just 1 result
                    results = s.Query<User>("index")
                        .Where(u => u.Email == "same.email@example.com")
                        .ToArray();
                    Assert.Equal(2, results.Length);*/
                }
            }
        }

        [Fact(Skip = "TODO : NumberOfCachedRequests Not Implemented")]
        public void Will_refresh_result_when_included_document_changes()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenNewSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1" });
                    s.SaveChanges();
                }

                using (var s = store.OpenNewSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                }

                using (var s = store.OpenNewSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    Assert.Equal(1, s.Advanced.NumberOfRequests);
                    s.Load<User>("users/1").Name = "foo";
                    s.SaveChanges();
                }


                using (var s = store.OpenNewSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests); // did NOT increase cache
                }
            }
        }
    }
}
