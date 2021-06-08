using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_123 : RavenTestBase
    {
        public RDBC_123(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Query_Using_an_Alias_on_Properties_of_Nested_Object_Array()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        ShipTo = new Address
                        {
                            City = "Paris",
                            Country = "France"
                        },
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Product = "products/1-A"
                            },
                            new OrderLine
                            {
                                Product = "products/5-A"
                            },
                            new OrderLine
                            {
                                Product = "products/9-A"
                            }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var projection = session.Advanced.RawQuery<dynamic>("from Orders as o select o.ShipTo.City, o.Lines[].Product");

                    var projectionResult = projection.ToList();

                    Assert.Equal(1, projectionResult.Count);

                    JObject res = projectionResult[0];
                    var city = res["ShipTo.City"].Value<string>();
                    var products = res["Lines[].Product"].Value<JArray>();

                    Assert.Equal("Paris", city);
                    Assert.Equal(3, products.Count);
                    Assert.Equal("products/1-A", products[0]);
                    Assert.Equal("products/5-A", products[1]);
                    Assert.Equal("products/9-A", products[2]);

                }
            }
        }
    }
}
