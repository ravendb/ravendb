using System;
using System.Collections.Generic;
using Raven.Client;
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
                store.OnBeforeStore += eventTest1;
                using (var newSession = store.OpenNewSession())
                {
                    newSession.Store(new User()
                    {
                        Name = "Toli",
                        Count = 1
                    } 
                    , "users/1");

                    newSession.OnBeforeStore += eventTest2;

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

       private void eventTest1(object sender, BeforeStoreEventArgs e)
        {
            var user = e.Entity as User;
            if (user != null)
            {
                user.Count = 1000;
            }
            e.DocumentMetadata["Nice"] = "true";
        }

        private void eventTest2(object sender, BeforeStoreEventArgs e)
        {
            var user = e.Entity as User;
            if (user != null)
            {
                user.LastName = "ravendb";
            }
        }

    }
}
