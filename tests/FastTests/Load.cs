using System.Collections.Generic;
using Raven.Tests.Core.Utils.Entities;

using Xunit;

namespace NewClientTests.NewClient
{
    public class Load :  RavenTestBase
    {
        [Fact]
        public void Load_Document_By_id()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                }
            }
        }

        [Fact]
        public void Load_Documents_By_ids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var user = newSession.Load<User>(new[] { "users/1", "users/2" });
                    Assert.Equal(user.Length, 2);
                }
            }
        }

        [Fact]
        public void Load_Document_By_ValueType_id()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var user = newSession.Load<User>(2);
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "Hibernating Rhinos");
                }
            }
        }

        [Fact]
        public void Load_Documents_By_ValueType_ids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var users = newSession.Load<User>(1, 2);
                    Assert.Equal(users.Length, 2);
                }
            }
        }

        [Fact]
        public void Load_Documents_By_IEnumerable_ValueType_ids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var users = newSession.Load<User>(new List<System.ValueType> { 1, 2 });
                    Assert.Equal(users.Length, 2);

                }
            }
        }

        [Fact]
        public void Load_Null_Should_Return_Null()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Tony Montana" }, "users/1");
                    session.Store(new User { Name = "Tony Soprano" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var user1 = newSession.Load<User>((string) null);
                    Assert.Null(user1);

                    var orderedArrayOfIdsWithNull = new[] {"users/1", null, "users/2", null};
                    var users1 = newSession.Load<User>(orderedArrayOfIdsWithNull);
                    Assert.Equal(users1[0].Id, "users/1");
                    Assert.Null(users1[1]);
                    Assert.Equal(users1[2].Id, "users/2");
                    Assert.Null(users1[3]);
                    
                    var unorderedSetOfIdsWithNull = new HashSet<string>() { "users/1", null, "users/2", null };
                    var users2 = newSession.Load<User>(unorderedSetOfIdsWithNull);
                    Assert.Equal(users2.Length, 3);
                    Assert.True(users2[0]==null || users2[1] == null || users2[2] == null);
                }
            }
        }
    }
}
