using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client
{
    public class ExistsAsync : RavenTestBase
    {
        [Fact]
        public async void CheckIfDocumentExists()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "Idan" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Shalom" }, "users/2");
                    await asyncSession.SaveChangesAsync();
                }

                using (var asyncSession = store.OpenAsyncSession())
                {
                    Assert.True(await asyncSession.ExistsAsync("users/1"));
                    Assert.False(await asyncSession.ExistsAsync("users/10"));
                    await asyncSession.LoadAsync<User>("users/2");
                    Assert.True(await asyncSession.ExistsAsync("users/2"));
                }
            }
        }
    }
}
