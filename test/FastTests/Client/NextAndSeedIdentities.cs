using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client
{
    public class NextAndSeedIdentities : RavenTestBase
    {
        [Fact]
        public async Task NextIdentityFor()
        {
            using (var store = GetDocumentStore())
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

                using (var commands = store.Commands())
                {
                    var command = new NextIdentityForCommand("users");

                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);
                }

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

        [Fact]
        public async Task SeedIdentityFor()
        {
            using (var store = GetDocumentStore())
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

                using (var commands = store.Commands())
                {
                    var command = new SeedIdentityForCommand("users", 1990);

                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                    var result = command.Result;

                    Assert.Equal(1990, result);
                }

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

                using (var commands = store.Commands())
                {
                    var command = new SeedIdentityForCommand("users", 1975);

                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                    var result = command.Result;

                    Assert.Equal(1991, result);
                }
                using (var commands = store.Commands())
                {
                    var command = new SeedIdentityForCommand("users", 1975, forced:true);

                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                    var result = command.Result;

                    Assert.Equal(1975, result);
                }
            }
        }
    }
}
