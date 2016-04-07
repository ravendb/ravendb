using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Tests.Core;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Queries
{
    public class DynamicMapReduceTests : RavenTestBase
    {
        [Fact]
        public async Task Group_by_string_calculate_count()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Address()
                    {
                        City = "Torun"
                    });
                    await session.StoreAsync(new Address()
                    {
                        City = "Torun"
                    });
                    await session.StoreAsync(new Address()
                    {
                        City = "Hadera"
                    });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var addresses = session.Query<Address>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.City).Select(
                        x =>
                        new {
                            City = x.Key,
                            Count = x.Count()
                        })
                        .Where(x => x.Count == 2)
                        .ToList();

                    Assert.Equal(2, addresses[0].Count);
                    Assert.Equal("Torun", addresses[0].City);

                    //var addresses2 =
                    //    session.Query<Address>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.City, x => 1,
                    //        (key, g) => new
                    //        {
                    //            City = key,
                    //            Count = g.Count()
                    //        }).Where(x => x.Count == 2)
                    //        .ToList();
                }
            }
        }
    }
}