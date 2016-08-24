using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Json.Linq;
using Xunit;
using System;
using Raven.Abstractions.Util;

namespace FastTests.Client
{
    public class IncludesUtilTests : RavenTestBase
    {
        [Fact]
        public async Task include_with_prefix()
        {
            using (var store = GetDocumentStore())
            {
                var order = RavenJObject.FromObject(new Order()
                {
                    CustomerId = "1",
                    Number = "abc"
                });
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(order, "orders/1");
                    await session.SaveChangesAsync();
                }

                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludesUtil.Include(order, "CustomerId(customer/)", CustomerId =>
                {
                    if (CustomerId == null)
                        return false;
                    ids.Add(CustomerId);
                    return true;
                });

                Assert.Equal(new[] { "customer/1", "1" }, ids);
            }
        }

        [Fact]
        public async Task include_with_suffix()
        {
            using (var store = GetDocumentStore())
            {
                var order = RavenJObject.FromObject(new Order()
                {
                    CustomerId = "1",
                    Number = "abc"
                });
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(order, "orders/1");
                    await session.SaveChangesAsync();
                }

                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludesUtil.Include(order, "CustomerId[{0}/customer]", CustomerId =>
                {
                    if (CustomerId == null)
                        return false;
                    ids.Add(CustomerId);
                    return true;
                });

                Assert.Equal(new[] { "1/customer", "1" }, ids);
            }
        }

        public class Order
        {
            public string Number { get; set; }
            public string CustomerId { get; set; }
        }
    }
}
