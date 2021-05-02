using System;
using System.Collections.Generic;
using FastTests;
using FastTests.Client;
using Raven.Client.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Core.Session
{
    public class AddOrPatch: RavenTestBase
    {
        public AddOrPatch(ITestOutputHelper output) : base(output)
        {
            
            
        }
        [Fact]
        public void CanAddOrPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var id = "users/1";
                    session.Store( new User
                    {
                        FirstName = "Hibernating",
                        LastName = "Rhinos",
                        LastLogin = DateTime.Now
                    }, id);
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
                using (var session = store.OpenSession())
                {
                    session.Advanced.AddOrPatch<User, DateTime>(
                        "users/1", 
                        new User
                        {
                            FirstName = "Hibernating",
                            LastName = "Rhinos",
                            LastLogin = DateTime.Now
                        },
                        x => x.LastLogin,new DateTime(1993,9,12));
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
        [Fact]
        public void CanAddOrPatchAddItemToAnExistingArray()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var id = "users/1";
                    session.Store( 
                        new User
                    {
                        FirstName = "Hibernating",
                        LastName = "Rhinos",
                        LastLogin = new DateTime(2011,02,01)
                    }, id);
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
                using (var session = store.OpenSession())
                {
                    session.Advanced.AddOrPatch<User, DateTime>(
                        "users/1",
                        new User {
                            FirstName = "Hibernating", 
                            LastName = "Rhinos", 
                            LoginTimes = 
                            new List<DateTime>
                            {
                                DateTime.UtcNow
                            }},
                        x => x.LoginTimes, 
                        u => u.Add(DateTime.Now));
                    
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
        private class User
        {
            public DateTime LastLogin { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public List<DateTime> LoginTimes { get; set; }
        }
    }

}
