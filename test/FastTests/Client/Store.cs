using System;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class Store : RavenTestBase
    {
        public Store(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Store_Document()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "RavenDB" }, "users/1");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                }
            }
        }

        [Fact]
        public void Store_Documents()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "RavenDB" }, "users/1");
                    newSession.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>(new[] { "users/1", "users/2" });
                    Assert.Equal(user.Count, 2);
                }
            }
        }

        [Fact]
        public void Refresh_stored_document()
        {
            using (var store = GetDocumentStore())
            {
                using (var outerSession = (DocumentSession) store.OpenSession())
                {
                    var user = new User
                    {
                        Name = "RavenDB"
                    };
                    outerSession.Store(user, "users/1");
                    outerSession.SaveChanges();

                    var changeVector = outerSession.Advanced.GetChangeVectorFor(user);
                    var lastModified = outerSession.Advanced.GetLastModifiedFor(user);

                    using (var innerSession = store.OpenSession())
                    {
                        var loadedUser = innerSession.Load<User>("users/1");
                        loadedUser.Age = 10;
                        innerSession.SaveChanges();
                    }
                    
                    Assert.True(outerSession.DocumentsById.TryGetValue("users/1", out DocumentInfo docInfo));
                 
                    outerSession.Advanced.Refresh(user);
                    
                    Assert.NotNull(docInfo.ChangeVector);
                    Assert.NotEqual(docInfo.GetLastModified(), DateTime.MinValue);
                    Assert.NotEqual(docInfo.ChangeVector, changeVector);
                    Assert.NotEqual(docInfo.GetLastModified(), lastModified);
                    Assert.NotEqual(outerSession.Advanced.GetChangeVectorFor(user), changeVector);
                    Assert.NotEqual(outerSession.Advanced.GetLastModifiedFor(user), lastModified);
                }
            }
        }

       /* [Fact]
        public void Store_Document_without_id_prop()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var user1 = new UserName { Name = "RavenDB"};
                    newSession.Store(user1);
                    newSession.SaveChanges();
                   
                }
            }
        }

        public class UserName
        {
            public string Name { get; set; }
        }*/
    }
}

