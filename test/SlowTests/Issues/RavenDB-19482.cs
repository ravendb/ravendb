using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19482 : RavenTestBase
{
    public RavenDB_19482(ITestOutputHelper output) : base(output)
    {
    }
    private class MyUser
    {
        public string Name { get; set; }
        public string Order { get; set; }
    }

    [Fact]
    public void Can_Project_Into()
    {
        using (var store = GetDocumentStore())
        {
            const string name = "Grisha";
            const string order = "orders/1";

            using (var session = store.OpenSession())
            {
                session.Store(new MyUser
                {
                    Name = name,
                    Order = order
                });
                session.SaveChanges();
            }
            
            using (var session = store.OpenSession())
            {
                var query = session.Query<MyUser>().ProjectInto<MyUser>();
                
                var result = query.ToList();

                Assert.Equal(name, result[0].Name);
                Assert.Equal(order, result[0].Order);
            }
        }
    }
}
