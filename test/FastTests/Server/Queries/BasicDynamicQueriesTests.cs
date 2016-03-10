using System.Linq;
using System.Threading.Tasks;
using Raven.Tests.Core;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Queries
{
    public class BasicDynamicQueriesTests : RavenTestBase
    {
        [Fact]
        public async Task Dynamic_query_with_simple_string_where_clause()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }
                
                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "Arek").ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Arek", users[0].Name);
                }
            }
        }

        [Fact]
        public async Task Dynamic_query_with_simple_numeric_where_clause()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak", Age = 40 });
                    await session.StoreAsync(new User { Name = "Arek", Age = 50 });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age > 40).ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Arek", users[0].Name);
                }
            }
        }

        [Fact]
        public async Task Dynamic_query_with_simple_where_clause_and_sorting()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Arek", Age = 25 }, "users/1");
                    await session.StoreAsync(new User { Name = "Jan", Age = 27 }, "users/2");
                    await session.StoreAsync(new User { Name = "Arek", Age = 39 }, "users/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var usersSortedByAge = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "Arek").OrderBy(x => x.Age).ToList();

                    Assert.Equal(2, usersSortedByAge.Count);
                    Assert.Equal("users/1", usersSortedByAge[0].Id);
                    Assert.Equal("users/3", usersSortedByAge[1].Id);
                }

                using (var session = store.OpenSession())
                {
                    var usersSortedByAge = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "Arek").OrderByDescending(x => x.Age).ToList();

                    Assert.Equal(2, usersSortedByAge.Count);
                    Assert.Equal("users/3", usersSortedByAge[0].Id);
                    Assert.Equal("users/1", usersSortedByAge[1].Id);
                }
            }
        }

        [Fact]
        public async Task Dynamic_query_with_sorting_by_doubles()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Camera { Megapixels = 1.3 }, "cameras/1");
                    await session.StoreAsync(new Camera { Megapixels = 0.5 }, "cameras/2");
                    await session.StoreAsync(new Camera { Megapixels = 1.0 }, "cameras/3");
                    await session.StoreAsync(new Camera { Megapixels = 2.0 }, "cameras/4");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var cameras = session.Query<Camera>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Megapixels).ToList();
                    
                    Assert.Equal("cameras/2", cameras[0].Id);
                    Assert.Equal("cameras/3", cameras[1].Id);
                    Assert.Equal("cameras/1", cameras[2].Id);
                    Assert.Equal("cameras/4", cameras[3].Id);
                }
            }
        }

        [Fact]
        public async Task Dynamic_query_with_sorting_by_strings()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "David" }, "users/1");
                    await session.StoreAsync(new User { Name = "Adam" }, "users/2");
                    await session.StoreAsync(new User { Name = "John" }, "users/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Name).ToList();

                    Assert.Equal("users/2", users[0].Id);
                    Assert.Equal("users/1", users[1].Id);
                    Assert.Equal("users/3", users[2].Id);

                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderByDescending(x => x.Name).ToList();

                    Assert.Equal("users/3", users[0].Id);
                    Assert.Equal("users/1", users[1].Id);
                    Assert.Equal("users/2", users[2].Id);
                }
            }
        }

        [Fact]
        public async Task Dynamic_query_partial_match()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "David", Age = 31}, "users/1");
                    await session.StoreAsync(new User { Name = "Adam", Age = 12}, "users/2");
                    await session.StoreAsync(new User { Name = "John", Age = 24}, "users/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Name).ToList();

                    Assert.Equal("users/2", users[0].Id);
                    Assert.Equal("users/1", users[1].Id);
                    Assert.Equal("users/3", users[2].Id);

                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age > 20).ToList();

                    Assert.Equal(2, users.Count);
                    Assert.Equal("users/1", users[0].Id);
                    Assert.Equal("users/3", users[1].Id);
                }
            }
        }
    }
}