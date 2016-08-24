using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Weave : RavenTestBase
    {
        public class CalcSystem
        {
            public int ClientID { get; set; }
            public int SystemID { get; set; }
            public int InstanceID { get; set; }
            public string Server { get; set; }
        }

        [Fact]
        public async Task Main()
        {
            using (var Store = await GetDocumentStore())
            {

                using (var session = Store.OpenSession())
                {
                    foreach (var sys in session.Query<CalcSystem>())
                    {
                        session.Delete(sys);
                    }
                    session.SaveChanges();
                }

                using (var session = Store.OpenSession())
                {
                    session.Store(new CalcSystem
                    {
                        ClientID = 5,
                        SystemID = 1,
                        InstanceID = 1,
                        Server = "TestServer"
                    });
                    session.Store(new CalcSystem
                    {
                        ClientID = 5,
                        SystemID = 2,
                        InstanceID = 1,
                        Server = "TestServer"
                    });
                    session.Store(new CalcSystem
                    {
                        ClientID = 5,
                        SystemID = 3,
                        InstanceID = 1,
                        Server = "TestServer2"
                    });

                    session.SaveChanges();
                }
                using (var session = Store.OpenSession())
                {
                    var allSystems = session.Query<CalcSystem>()
                        .Customize(x=>x.WaitForNonStaleResultsAsOfNow())
                        .Where(c => c.Server != "");
                    var distinctSystems = allSystems.Select(m => m.Server).Distinct();
                    Assert.Equal(distinctSystems.ToList().Count, 2);
                }
            }
        }
    }
}
