using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core.Session
{
    public class OptimisticConcurrency : RavenCoreTestBase
    {
#if DNXCORE50
        public Advanced(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif
        [Fact]
        public void CanUseOptmisticConcurrency()
        {
            const string entityId = "users/1";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.UseOptimisticConcurrency);
                    session.Advanced.UseOptimisticConcurrency = true;

                    session.Store(new User { Id = entityId, Name = "User1" });
                    session.SaveChanges();

                    using (var otherSession = store.OpenSession())
                    {
                        var otherUser = otherSession.Load<User>(entityId);
                        otherUser.Name = "OtherName";
                        otherSession.Store(otherUser);
                        otherSession.SaveChanges();
                    }

                    var user = session.Load<User>("users/1");
                    user.Name = "Name";
                    session.Store(user);
                    var e = Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                    Assert.Equal("PUT attempted on document '" + entityId + "' using a non current etag", e.Message);
                }
            }
        }

        [Fact]
        public void CanBypassOptmisticConcurrencyCheckByExplicitlyProvidingAnEtagOfNull()
        {
            const string entityId = "users/1";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    session.Store(new User { Id = entityId, Name = "User1" });
                    session.SaveChanges();

                    using (var otherSession = store.OpenSession())
                    {
                        var otherUser = otherSession.Load<User>(entityId);
                        otherUser.Name = "OtherName";
                        otherSession.Store(otherUser);
                        otherSession.SaveChanges();
                    }

                    var user = session.Load<User>("users/1");
                    user.Name = "Name";
                    session.Store(user, (Etag)null);
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void CanBypassOptmisticConcurrencyCheckByExplicitlyProvidingAnEtagOfNullToStore()
        {
            const string entityId = "users/1";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    session.Store(new User { Id = entityId, Name = "User1" });
                    session.SaveChanges();

                    using (var otherSession = store.OpenSession())
                    {
                        var otherUser = otherSession.Load<User>(entityId);
                        otherUser.Name = "OtherName";
                        otherSession.Store(otherUser);
                        otherSession.SaveChanges();
                    }

                    var user = session.Load<User>("users/1");
                    user.Name = "Name";
                    session.Store(user, (Etag)null);
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void CanBypassOptmisticConcurrencyCheckByExplicitlyProvidingAnEtagAndAnIdOfNullToStore()
        {
            const string entityId = "users/1";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    session.Store(new User { Id = entityId, Name = "User1" });
                    session.SaveChanges();

                    using (var otherSession = store.OpenSession())
                    {
                        var otherUser = otherSession.Load<User>(entityId);
                        otherUser.Name = "OtherName";
                        otherSession.Store(otherUser);
                        otherSession.SaveChanges();
                    }

                    var user = session.Load<User>("users/1");
                    user.Name = "Name";
                    session.Store(user, (Etag)null);
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public async Task CanBypassOptmisticConcurrencyCheckByExplicitlyProvidingAnEtagOfNullToStoreAsync()
        {
            const string entityId = "users/1";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    await session.StoreAsync(new User { Id = entityId, Name = "User1" });
                    await session.SaveChangesAsync();

                    using (var otherSession = store.OpenSession())
                    {
                        var otherUser = otherSession.Load<User>(entityId);
                        otherUser.Name = "OtherName";
                        otherSession.Store(otherUser);
                        otherSession.SaveChanges();
                    }

                    var user = await session.LoadAsync<User>("users/1");
                    user.Name = "Name";
                    await session.StoreAsync(user, null, entityId);
                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task CanBypassOptmisticConcurrencyCheckByExplicitlyProvidingAnEtagAndAnIdOfNullToStoreAsync()
        {
            const string entityId = "users/1";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    await session.StoreAsync(new User { Id = entityId, Name = "User1" });
                    await session.SaveChangesAsync();

                    using (var otherSession = store.OpenSession())
                    {
                        var otherUser = otherSession.Load<User>(entityId);
                        otherUser.Name = "OtherName";
                        otherSession.Store(otherUser);
                        otherSession.SaveChanges();
                    }

                    var user = await session.LoadAsync<User>("users/1");
                    user.Name = "Name";
                    await session.StoreAsync(user, null, entityId);
                    await session.SaveChangesAsync();
                }
            }
        }
    }
}
