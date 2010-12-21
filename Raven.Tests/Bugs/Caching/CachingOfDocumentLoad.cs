//-----------------------------------------------------------------------
// <copyright file="CachingOfDocumentLoad.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Client.Client;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs.Caching
{
    public class CachingOfDocumentLoad : RemoteClientTest
    {
        public CachingOfDocumentLoad()
        {
            HttpJsonRequest.ResetCache();
        }

        [Fact]
        public void Can_cache_document_load()
        {
            using (GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
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
                    Assert.Equal(1, HttpJsonRequest.NumberOfCachedRequests);
                }
            }
        }

        [Fact]
        public void After_modification_will_get_value_from_server()
        {
            using (GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
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
                    Assert.Equal(1, HttpJsonRequest.NumberOfCachedRequests);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<User>("users/1");
                    Assert.Equal(1, HttpJsonRequest.NumberOfCachedRequests); // did NOT get from cache
                }
            }
        }
    }
}