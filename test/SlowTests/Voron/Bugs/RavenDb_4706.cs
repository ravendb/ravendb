using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class RavenDb_4706 : RavenTestBase
    {
        public RavenDb_4706(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task SupportRandomOrder()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak Yitzchaki" });
                    await session.StoreAsync(new User { Name = "Oren Eini" });
                    await session.StoreAsync(new User { Name = "Maxim Buryak" });
                    await session.StoreAsync(new User { Name = "Grisha Kotler" });
                    await session.StoreAsync(new User { Name = "Michael Yarichuk" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Query<User>()
                        .Customize(customization => customization.RandomOrdering())
                        .Where(product => product.Name != "Fitzchak Yitzchaki")
                        .Take(2)
                        .ToListAsync();

                    Assert.Equal(2, users.Count);
                }
            }
        }
    }
}
