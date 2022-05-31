using System;
using System.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class TrackEntity : RavenTestBase
    {
        public TrackEntity(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Deleting_Entity_That_Is_Not_Tracked_Should_Throw()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
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
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = "users/1", Name = "John" });
                    session.Store(new User { Id = "users/2", Name = "Jonathan" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.Delete("users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
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
                using (var session = store.OpenSession())
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

        [Fact]
        public void Get_Tracked_Entities()
        {
            using (var store = GetDocumentStore())
            {
                string userId;
                string companyId;

                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "Grisha" };
                    session.Store(user);
                    userId = user.Id;
                    var company = new Company { Name = "Hibernating Rhinos" };
                    session.Store(company);
                    companyId = company.Id;
                    var order = new Order { Employee = company.Id };
                    session.Store(order);

                    var tracked = session.Advanced.GetTrackedEntities();

                    tracked.TryGetValue(userId, out EntityInfo value);
                    Assert.NotNull(value);
                    Assert.Equal(userId, value.Id);
                    Assert.True(value.Entity is User);

                    tracked.TryGetValue(company.Id, out value);
                    Assert.NotNull(value);
                    Assert.Equal(companyId, value.Id);
                    Assert.True(value.Entity is Company);

                    tracked.TryGetValue(order.Id, out value);
                    Assert.NotNull(value);
                    Assert.Equal(order.Id, value.Id);
                    Assert.True(value.Entity is Order);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(userId);
                    session.Delete(companyId);

                    var tracked = session.Advanced.GetTrackedEntities();
                    Assert.Equal(2, tracked.Count);
                    Assert.True(tracked[userId].IsDeleted);
                    Assert.True(tracked[companyId].IsDeleted);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(userId);
                    session.Delete(companyId);

                    var usersLazy = session.Advanced.Lazily.LoadStartingWith<User>("u");
                    var users = usersLazy.Value;
                    Assert.Null(users.First().Value);

                    var company = session.Load<Company>(companyId);
                    Assert.Null(company);

                    var tracked = session.Advanced.GetTrackedEntities();
                    Assert.Equal(2, tracked.Count);
                    Assert.True( tracked[userId].IsDeleted);
                    Assert.True( tracked[companyId].IsDeleted);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(userId);
                    session.Delete(user.Id);
                    var tracked = session.Advanced.GetTrackedEntities();
                    Assert.Equal(1, tracked.Count);
                    Assert.Equal(userId, tracked.First().Key);
                    Assert.True(tracked.First().Value.IsDeleted);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(userId);
                    session.Delete(user.Id.ToUpper());
                    var tracked = session.Advanced.GetTrackedEntities();
                    Assert.Equal(1, tracked.Count);
                    Assert.Equal(userId, tracked.First().Key, StringComparer.OrdinalIgnoreCase);
                    Assert.True(tracked.First().Value.IsDeleted);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(userId);
                    session.Delete(user);
                    var tracked = session.Advanced.GetTrackedEntities();
                    Assert.Equal(1, tracked.Count);
                    Assert.Equal(userId, tracked.First().Key);
                    Assert.True(tracked.First().Value.IsDeleted);
                }
            }
        }
    }
}
