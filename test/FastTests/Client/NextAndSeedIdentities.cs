using Raven.Client.Documents.Operations.Identities;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class NextAndSeedIdentities : RavenTestBase
    {
        public NextAndSeedIdentities(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void NextIdentityFor(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    var entity = new User
                    {
                        LastName = "Adi"
                    };

                    s.Store(entity, "users|");
                    s.SaveChanges();
                }

                store.Maintenance.Send(new NextIdentityForOperation("users"));

                using (var s = store.OpenSession())
                {
                    var entity = new User
                    {
                        LastName = "Avivi"
                    };

                    s.Store(entity, "users|");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var entityWithId1 = s.Load<User>("users/1");
                    var entityWithId2 = s.Load<User>("users/2");
                    var entityWithId3 = s.Load<User>("users/3");
                    var entityWithId4 = s.Load<User>("users/4");

                    Assert.NotNull(entityWithId1);
                    Assert.NotNull(entityWithId3);
                    Assert.Null(entityWithId2);
                    Assert.Null(entityWithId4);

                    Assert.Equal("Adi", entityWithId1.LastName);
                    Assert.Equal("Avivi", entityWithId3.LastName);
                }
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void SeedIdentityFor(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    var entity = new User
                    {
                        LastName = "Adi"
                    };

                    s.Store(entity, "users|");
                    s.SaveChanges();
                }

                var result1 = store.Maintenance.Send(new SeedIdentityForOperation("users", 1990));

                Assert.Equal(1990, result1);

                using (var s = store.OpenSession())
                {
                    var entity = new User
                    {
                        LastName = "Avivi"
                    };

                    s.Store(entity, "users|");
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var entityWithId1 = s.Load<User>("users/1");
                    var entityWithId2 = s.Load<User>("users/2");
                    var entityWithId1990 = s.Load<User>("users/1990");
                    var entityWithId1991 = s.Load<User>("users/1991");
                    var entityWithId1992 = s.Load<User>("users/1992");

                    Assert.NotNull(entityWithId1);
                    Assert.NotNull(entityWithId1991);
                    Assert.Null(entityWithId2);
                    Assert.Null(entityWithId1990);
                    Assert.Null(entityWithId1992);

                    Assert.Equal("Adi", entityWithId1.LastName);
                    Assert.Equal("Avivi", entityWithId1991.LastName);
                }

                var result2 = store.Maintenance.Send(new SeedIdentityForOperation("users", 1975));
                Assert.Equal(1991, result2);

                var result3 = store.Maintenance.Send(new SeedIdentityForOperation("users", 1975, forceUpdate: true));
                Assert.Equal(1975, result3);
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void NextIdentityForOperationShouldCreateANewIdentityIfThereIsNone(Options options)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var result = store.Maintenance.Send(new NextIdentityForOperation("person|"));

                    Assert.Equal(1, result);
                }
            }
        }
    }
}
