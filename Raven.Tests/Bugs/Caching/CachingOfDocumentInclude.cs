//-----------------------------------------------------------------------
// <copyright file="CachingOfDocumentInclude.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Client.Client;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs.Caching
{
    public class CachingOfDocumentInclude : RemoteClientTest
    {
        public CachingOfDocumentInclude()
        {
            HttpJsonRequest.ResetCache();
        }

        [Fact]
        public void Can_cache_document_with_includes()
        {
            using (GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User {Name = "Ayende"});
                    s.Store(new User { PartnerId = "users/1"});
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x=>x.PartnerId)
                        .Load("users/2");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    Assert.Equal(1, HttpJsonRequest.NumberOfCachedRequests);
                }
            }
        }

        [Fact]
        public void Will_referesh_result_when_main_document_changes()
        {
            using (GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var user = s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    Assert.Equal(1, HttpJsonRequest.NumberOfCachedRequests);
                    user.Name = "Foo";
                    s.SaveChanges();
                }


                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    Assert.Equal(1, HttpJsonRequest.NumberOfCachedRequests); // did NOT increase cache
                }
            }
        }

        [Fact]
        public void Will_referesh_result_when_included_document_changes()
        {
            using (GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { PartnerId = "users/1" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                }

                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    Assert.Equal(1, HttpJsonRequest.NumberOfCachedRequests);
                    s.Load<User>("users/1").Name = "foo";
                    s.SaveChanges();
                }


                using (var s = store.OpenSession())
                {
                    s.Include<User>(x => x.PartnerId)
                        .Load("users/2");
                    Assert.Equal(1, HttpJsonRequest.NumberOfCachedRequests); // did NOT increase cache
                }
            }
        }
    }
}