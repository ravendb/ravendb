using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Counters;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15282 : RavenTestBase
    {
        public RavenDB_15282(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CountersPostGetReturnFullResults()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "users/1";
                string[] counterNames = new string[1000];

                using (var session = store.OpenSession())
                {
                    session.Store(new object(), docId);

                    var c = session.CountersFor(docId);

                    for (int i = 0; i < 1000; i++)
                    {
                        string name = $"likes{i}";
                        counterNames[i] = name;
                        c.Increment(name);
                    }

                    session.SaveChanges();
                }

                var vals = store.Operations.Send(new GetCountersOperation(docId, counterNames, returnFullResults: true));
                Assert.Equal(1000, vals.Counters.Count);

                for (int i = 0; i < 1000; i++)
                {
                    Assert.Equal(1, vals.Counters[i].CounterValues.Count); 
                    Assert.Equal(1, vals.Counters[i].CounterValues.Values.First());
                }
            }
        }
    }
}
