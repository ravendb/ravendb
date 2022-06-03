using System;
using System.Linq;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace FastTests.Corax;

public class AnalyzersIntegration : RavenTestBase
{
    public AnalyzersIntegration(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void LowercaseAnalyzer(Options options)
    {
        var storedValue = "TeSTeR";
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Record() { Data = storedValue });
            session.SaveChanges();
        }
        {
            using var session = store.OpenSession();
            var items = session.Query<Record>().Where(x => x.Data == "tester").ToList();

            Assert.NotNull(items);
            Assert.NotEqual(0, items.Count);
            Assert.Equal(storedValue, items.First().Data);
        }
        {
            using var session = store.OpenSession();
            var items = session.Query<Record>().Where(x => x.Data == "tEstEr").ToList();

            Assert.NotNull(items);
            Assert.NotEqual(0, items.Count);
            Assert.Equal(storedValue, items.First().Data);
        }
    }

    [Theory]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void RavenStandardAnalyzer(Options options)
    {
        var valuesToStore = new[] { "PoINt oF tHe IMPLemeNTation", "tHiS IS TesTion" };
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Record() { Data = valuesToStore[0] });
            session.Store(new Record() { Data = valuesToStore[1] });
            session.SaveChanges();
        }
        {
            using var session = store.OpenSession();
            var items = session.Query<Record>().Search(x => x.Data, "*tion").ToList();
            WaitForUserToContinueTheTest(store);
            Assert.Equal(2, items.Count);
        }
    }

    private class Record
    {
        public string Data { get; init; }
    }
}
