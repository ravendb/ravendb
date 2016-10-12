using Raven.Abstractions.Data;
using Raven.Client.Documents.Listeners;
using System;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace FastTests.NewClient
{
    public class Listeners : RavenTestBase
    {
        [Fact]
        public void Before_Store_Listerner()
        {
            using (var store = GetDocumentStore())
            {
                store.RegisterNewListener(new AuditListener());
                using (var newSession = store.OpenNewSession())
                {
                    newSession.Store(new User()
                    {
                        Name = "Toli",
                        Count = 1
                    } 
                    , "users/1");
                  
                    Assert.Equal(newSession.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
                using (var newSession = store.OpenNewSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.Equal(user.Count, 1000);
                }
            }
        }

        private class AuditListener : Raven.Client.Documents.Listeners.IDocumentStoreListener
        {
            public bool BeforeStore(string key, object entityInstance, BlittableJsonReaderObject metadata,
                BlittableJsonReaderObject original)
            {
                if (entityInstance is User)
                {
                    ((User) entityInstance).Count = 1000;
                    return true; //to indicate we changed something
                }
                return false;
            }

            public void AfterStore(string key, object entityInstance, BlittableJsonReaderObject metadata)
            {
                
            }
        }
    }
}
