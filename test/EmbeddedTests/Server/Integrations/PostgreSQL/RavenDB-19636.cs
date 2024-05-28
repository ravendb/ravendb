#if NET8_0
using System;
using System.Threading.Tasks;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL
{
    public class RavenDB_19636 : PostgreSqlIntegrationTestBase
    {

        private new class Order
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string ExternalId { get; set; }
        }

        [Fact]
        public async Task CanLoadCorrectlyWhenFirstRowHasNullValueInColumn()
        {
            string query = $"from Orders order by id()";

            using (var store = GetDocumentStore())
            {
                var order1 = new Order
                {
                    Id = "orders/1",
                    Name = null,
                };
                
                var order2 = new Order
                {
                    Id = "orders/2",
                    Name = "OrderTwo",
                };
                
                var order3 = new Order
                {
                    Id = "orders/3",
                    Name = "OrderThree",
                    ExternalId = "OT"
                };
                 
                using (var session = store.OpenSession())
                {
                    session.Store(order1);
                    session.Store(order2);
                    session.Store(order3);
                    session.SaveChanges();
                }

                var result = await Act(store, query);
                
                Assert.Equal(result.Rows[0].ItemArray[0], "orders/1");
                Assert.Equal(result.Rows[0].ItemArray[1], DBNull.Value);
                Assert.Equal(result.Rows[0].ItemArray[2], DBNull.Value);
                
                Assert.Equal(result.Rows[1].ItemArray[0], "orders/2");
                Assert.Equal(result.Rows[1].ItemArray[1], "OrderTwo");
                Assert.Equal(result.Rows[1].ItemArray[2], DBNull.Value); 
                
                Assert.Equal(result.Rows[2].ItemArray[0], "orders/3");
                Assert.Equal(result.Rows[2].ItemArray[1], "OrderThree");
                Assert.Equal(result.Rows[2].ItemArray[2], "OT");
            }
        }
    }
}
#endif
