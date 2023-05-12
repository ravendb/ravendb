using System.Linq;
using Nest;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class CoraxSorting : RavenTestBase
{
    public CoraxSorting(ITestOutputHelper output) : base(output)
    {
    }

    private static string[] Names = {
        "almond", "community", "mark", "user661", "bps", "knackeredcoder", "lnediger", "suhair", "emily", "user1324", "user1364", "user1416", "kbrinley", "pelleg",
        "tony abell", "tonyossa", "francis b.", "sushant", "tjofras", "user2146", "easyecho", "oneshot", "paul", "user2545", "nstehr", "scable", "user2623",
        "user2682", "another average joe", "david", "mara morton", "robert", "alex", "bryan woods", "markom", "user3377",
    };

    private record User(string Name);
    
    [Fact]
    public void CanProperlySort()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        using (var s = store.OpenSession())
        {
            foreach (var n in Names)
            {
                s.Store(new User(n));
            }
            s.SaveChanges();
        }

        using (var s = store.OpenSession())
        {
            s.Advanced.MaxNumberOfRequestsPerSession = 10000;
            for (int i = 0; i < Names.Length; i++)
            {
                var actual =s.Query<User>()
                    .Take(i)
                    .OrderBy(x => x.Name)
                    .Select(x => x.Name)
                    .ToArray();
                Assert.Equal(Names.Order().Take(i), actual);
            }
        }
    }


}
