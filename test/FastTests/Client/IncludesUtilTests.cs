using System.Collections.Generic;
using Xunit;
using System;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace FastTests.Client
{
    public class IncludesUtilTests : RavenTestBase
    {
        [Fact]
        public void include_with_prefix()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = EntityToBlittable.ConvertCommandToBlittable(new Order
                {
                    CustomerId = "1",
                    Number = "abc"
                }, context);

                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludesUtil.Include(json, "CustomerId(customer/)", customerId =>
                {
                    if (customerId == null)
                        return;

                    ids.Add(customerId);
                });

                Assert.Equal(new[] { "customer/1", "1" }, ids);
            }
        }

        [Fact]
        public void include_with_suffix()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = EntityToBlittable.ConvertCommandToBlittable(new Order
                {
                    CustomerId = "1",
                    Number = "abc"
                }, context);

                var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IncludesUtil.Include(json, "CustomerId[{0}/customer]", customerId =>
                {
                    if (customerId == null)
                        return;

                    ids.Add(customerId);
                });

                Assert.Equal(new[] { "1/customer", "1" }, ids);
            }
        }

        private class Order
        {
            public string Number { get; set; }
            public string CustomerId { get; set; }
        }
    }
}
