using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.NewClient
{
    public class TrackEntity : RavenTestBase
    {
        [Fact]
        public void Deleting_Entity_That_Is_Not_Tracked_Should_Throw()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var e = Assert.Throws<InvalidOperationException>(() => session.Delete(new User()));
                    Assert.Equal("Raven.Tests.Core.Utils.Entities.User is not associated with the session, cannot delete unknown entity instance", e.Message);
                }
            }
        }

        [Fact]
        public void Loading_Deleted_Document_Should_Return_Null()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new User { Id = "users/1", Name = "John" });
                    session.Store(new User { Id = "users/2", Name = "Jonathan" });
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    session.Delete("users/1");
                    session.Delete<User>(2);
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    Assert.Null(session.Load<User>("users/1"));
                    Assert.Null(session.Load<User>("users/2"));
                }
            }
        }

        [Fact]
        public void Storing_Document_With_The_Same_Id_In_The_Same_Session_Should_Throw()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var user = new User { Id = "users/1", Name = "User1" };

                    session.Store(user);
                    session.SaveChanges();

                    user = new User { Id = "users/1", Name = "User2" };

                    var e = Assert.Throws<NonUniqueObjectException>(() => session.Store(user));
                    Assert.Equal("Attempted to associate a different object with id 'users/1'.", e.Message);
                }
            }
        }

    }
}