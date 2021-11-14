using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9955 : RavenTestBase
    {
        public RavenDB_9955(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void JsConvertorShouldIgnoreValueProperyOfNullable(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {                
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName =  "Jerry",
                        LastName =  "Garcia",
                        Active = true
                    });
                    session.Store(new User
                    {
                        FirstName =  "Bob",
                        LastName =  "Weir"                        
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                        where u.Active != null
                        select new
                        {
                            Name = u.FirstName + " " + u.LastName,
                            Active = u.Active.Value
                        };
                    
                    Assert.Equal("from 'Users' as u where u.Active != $p0 " +
                                 "select { Name : u?.FirstName+\" \"+u?.LastName, Active : u?.Active }", query.ToString());
                    
                    var result = query.ToList();
                    
                    Assert.Equal(1, result.Count);
                    Assert.Equal("Jerry Garcia", result[0].Name);
                    Assert.True(result[0].Active);
                }
            }
        }
        
        [Theory]
        [JavaScriptEngineClassData]
        public void ToDictionaryWithNullableValue(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                var debtor = new Debtor()
                {
                    Name = "Jerry",
                    OrderDays = new List<OrderDay>()
                    {
                        new OrderDay
                        {
                            Name = "Shipment by boat on monday",
                            Day = OrderDays.Monday
                        },
                        new OrderDay
                        {
                            Name = "No day"
                        }
                    }
                };
                
                using (var session = store.OpenSession())
                {
                    session.Store(debtor);
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = from d in session.Query<Debtor>()
                                select new
                                {
                                    Name = d.Name,
                                    OrderDays = d.OrderDays.Where(a => a.Day != null).ToDictionary(a => a.Day.Value, a => a.Name)
                                };
                    
                    var result = query.ToList();                    
                    var days = result[0].OrderDays;
                    
                    Assert.Equal(1, days.Count);
                    Assert.True(days.TryGetValue(OrderDays.Monday, out var val));
                    Assert.Equal(val, "Shipment by boat on monday");

                }
            }
        }
        
        private enum OrderDays
        {
            Monday,
            Tuesday
        }

        private class OrderDay
        {
            public OrderDays? Day { get; set; }
            public string Name { get; set; }
        }

        private class Debtor
        {
            public string Name { get; set; }
            public IEnumerable<OrderDay> OrderDays { get; set; }
        }
        
        private class User
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public bool? Active { get; set; }           
        }
    }
}
