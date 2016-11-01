using System;
using System.Collections.Generic;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace FastTests.NewClient
{
    public class Events : RavenTestBase
    {
        [Fact]
        public void Before_Store_Listerner()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenNewSession())
                {
                    newSession.Store(new User()
                    {
                        Name = "Toli",
                        Count = 1
                    } 
                    , "users/1");

                    store.BeforeStoreEvent += eventTest1;
                    newSession.DocumentStore.BeforeStoreEvent += eventTest2;

                    Assert.Equal(newSession.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
                using (var newSession = store.OpenNewSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.Equal(user.Count, 1000);
                    Assert.Equal(user.LastName, "ravendb");
                    user.Age = 3;
                    newSession.SaveChanges();
                }
            }
        }

        private void eventTest1(InMemoryDocumentSessionOperations session, string id, object entityInstance, IDictionary<string, string> metadata)
        {
            if (entityInstance is User)
            {
                ((User)entityInstance).Count = 1000;
            }
        }

        private void eventTest2(InMemoryDocumentSessionOperations session, string id, object entityInstance, IDictionary<string, string> metadata)
        {
            if (entityInstance is User)
            {
                ((User)entityInstance).LastName = "ravendb";
            }
        }

    }
}
