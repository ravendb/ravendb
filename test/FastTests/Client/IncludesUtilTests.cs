using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class IncludesUtilTests : RavenTestBase
    {
        public IncludesUtilTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void include_with_prefix()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new Order
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
                var json = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new Order
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
