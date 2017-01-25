//-----------------------------------------------------------------------
// <copyright file="CachingOfDocumentLoad.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using Xunit;

namespace SlowTests.Bugs.Caching
{
    public class CachingOfDocumentLoad : RavenNewTestBase
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
        public void Can_cache_document_load()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<User>("users/1");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<User>("users/1");
                    Assert.Equal(1, s.Advanced.RequestExecuter.Cache.NumberOfItems);
                }
            }
        }

        [Fact]
        public void Can_NOT_cache_document_load()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.ShouldCacheRequest = s => false;

                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<User>("users/1");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<User>("users/1");
                    Assert.Equal(0, s.Advanced.RequestExecuter.Cache.NumberOfItems);
                }
            }
        }


        [Fact]
        public void After_modification_will_get_value_from_server()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<User>("users/1");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var user = s.Load<User>("users/1");
                    user.Name = "Rahien";
                    Assert.Equal(1, s.Advanced.RequestExecuter.Cache.NumberOfItems);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<User>("users/1");
                    Assert.Equal(1, s.Advanced.RequestExecuter.Cache.NumberOfItems); // did NOT get from cache
                }
            }
        }
    }
}
