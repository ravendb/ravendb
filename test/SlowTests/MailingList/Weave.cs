using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Weave : RavenTestBase
    {
        public Weave(ITestOutputHelper output) : base(output)
        {
        }

        private class CalcSystem
        {
            public int ClientID { get; set; }
            public int SystemID { get; set; }
            public int InstanceID { get; set; }
            public string Server { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Main(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    foreach (var sys in session.Query<CalcSystem>())
                    {
                        session.Delete(sys);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
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
                using (var session = store.OpenSession())
                {
                    var allSystems = session.Query<CalcSystem>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(c => c.Server != "");
                    var distinctSystems = allSystems.Select(m => m.Server).Distinct();
                    Assert.Equal(distinctSystems.ToList().Count, 2);
                }
            }
        }
    }
}
