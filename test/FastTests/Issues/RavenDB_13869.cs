using System;
using System.Collections.Generic;
using System.Text;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_13869:RavenTestBase
    {
        
        [Fact]
        public void MissingFieldsDataShouldBeCleared()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //person
                    session.Store(new User
                    {
                        Name = "John",
                        LastName = "Doh"
                    },"users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Load<Person>("users/1");
                    var sessionOperations = (session as InMemoryDocumentSessionOperations);                    
                    session.Advanced.Clear();

                    session.Store(person, "users/2");                    
                    session.SaveChanges();                    
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Load<User>("users/2");
                    Assert.Null(person.LastName);
                }
            }
        }

        [Fact]
        public void MissingFieldsDataShouldBeEvicted()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {                    
                    session.Store(new User
                    {
                        Name = "John",
                        LastName = "Doh"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Load<Person>("users/1");
                    var sessionOperations = (session as InMemoryDocumentSessionOperations);                    
                    session.Advanced.Evict(person);

                    session.Store(person, "users/2");                    
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Load<User>("users/2");
                    Assert.Null(person.LastName);
                }
            }
        }
    }
}
