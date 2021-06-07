using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
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
                var id = "users/1";
                using (var session = store.OpenSession())
                {
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
                        id, 
                        new User
                        {
                            FirstName = "Hibernating",
                            LastName = "Rhinos",
                            LastLogin = DateTime.Now
                        },
                        x => x.LastLogin,new DateTime(1993,9,12));
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    
                    session.Delete(id);
                    session.SaveChanges();
                    
                }
                using (var session = store.OpenSession())
                {
                    session.Advanced.AddOrPatch<User, DateTime>(
                        id, 
                        new User
                        {
                            FirstName = "Hibernating",
                            LastName = "Rhinos",
                            LastLogin = DateTime.MinValue
                        },
                        x => x.LastLogin,new DateTime(1993,9,12));
                    
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests); 
                    
                    var user = session.Load<User>(id);
                    Assert.Equal(user.FirstName,"Hibernating");
                    Assert.Equal(user.LastName,"Rhinos");
                    Assert.Equal(user.LastLogin,DateTime.MinValue);

                }
            }
        }
        
        [Fact]
        public void CanAddOrPatchAddItemToAnExistingArray()
        {
            
            using (var store = GetDocumentStore())
            {
                
                var id = "users/1";
                using (var session = store.OpenSession())
                {
                    session.Store( 
                        new User
                    {
                        FirstName = "Hibernating",
                        LastName = "Rhinos",
                        LoginTimes =new List<DateTime>
                        {
                        new DateTime(2000,01,02)
                        }
                    }, id);
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
                using (var session = store.OpenSession())
                {
                    
                    session.Advanced.AddOrPatch<User, DateTime>(
                        id,
                        new User {
                            FirstName = "Hibernating", 
                            LastName = "Rhinos", 
                            LoginTimes = 
                            new List<DateTime>
                            {
                                DateTime.UtcNow
                            }},
                        x => x.LoginTimes, 
                        u => u.Add(new DateTime(1993,09,12),new DateTime(2000,01,01)));
                    
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var user = session.Load<User>(id);
                    var dateTimes = new DateTime[]
                    {
                        new DateTime(2000, 01, 02),
                        new DateTime(1993, 09, 12),
                        new DateTime(2000, 01, 01)
                    };
                    Assert.Equal(user.LoginTimes.Select(dt=>dt.Date),dateTimes.Select(dt=>dt.Date));
                    
                    session.Delete(id);
                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    session.Advanced.AddOrPatch<User, DateTime>(
                        id, 
                        new User
                        {
                            FirstName = "Hibernating",
                            LastName = "Rhinos",
                            LastLogin = DateTime.MinValue
                        },
                        x => x.LastLogin,new DateTime(1993,9,12));
                    
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests); 
                    
                    var user = session.Load<User>(id);
                    Assert.Equal(user.FirstName,"Hibernating");
                    Assert.Equal(user.LastName,"Rhinos");
                    Assert.Equal(user.LastLogin,DateTime.MinValue);

                }
            }
        }
        
        [Fact]
        public void CanAddOrPatchIncrement()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                using (var session = store.OpenSession())
                {
                    
                    session.Store( 
                        new User
                        {
                            FirstName = "Hibernating",
                            LastName = "Rhinos",
                            LoginCount = 1
                            
                        }, id);
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {

                    session.Advanced.AddOrIncrement<User, int>(id, 
                        new User
                        {
                            FirstName = "Hibernating", 
                            LastName = "Rhinos", 
                            LoginCount = 1
                        
                        }, x => x.LoginCount, 3);

                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var user = session.Load<User>(id);
                    Assert.Equal(4,user.LoginCount);
                   
                    session.Delete(id);
                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    session.Advanced.AddOrPatch<User, DateTime>(
                        id, 
                        new User
                        {
                            FirstName = "Hibernating",
                            LastName = "Rhinos",
                            LastLogin = DateTime.MinValue
                        },
                        x => x.LastLogin,new DateTime(1993,9,12));
                    
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests); 
                    
                    var user = session.Load<User>(id);
                    Assert.Equal(user.FirstName,"Hibernating");
                    Assert.Equal(user.LastName,"Rhinos");
                    Assert.Equal(user.LastLogin,DateTime.MinValue);

                }
            }
        }
        
        private class User
        {
            public DateTime LastLogin { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public List<DateTime> LoginTimes { get; set; }
            public int LoginCount { get; set; }
        }
    }

}
