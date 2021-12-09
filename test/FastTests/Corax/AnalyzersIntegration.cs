using System;
using System.Linq;
using Raven.Client.Documents.Linq;
using FastTests.Server.Documents.Indexing;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class AnalyzersIntegration : RavenTestBase
{
    public AnalyzersIntegration(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [SearchEngineClassData(SearchEngineType.Corax)]
    public void LowercaseAnalyzer(string searchEngineType)
    {
        var storedValue = "TeSTeR";
        using var store = GetDocumentStore(Options.ForSearchEngine(searchEngineType));
        {
            using var session = store.OpenSession();
            session.Store(new Record(){Data = storedValue});
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
    [SearchEngineClassData(SearchEngineType.Corax)]
    public void RavenStandardAnalyzer(string searchEngineType)
    {
        var valuesToStore = new[] { "PoINt oF tHe IMPLemeNTation", "tHiS IS TesT" };
        using var store = GetDocumentStore(Options.ForSearchEngine(searchEngineType));
        {
            using var session = store.OpenSession();
            session.Store(new Record() { Data = valuesToStore[0] });
            session.Store(new Record() { Data = valuesToStore[1] });
            session.SaveChanges();
        }
        {
            var notImplementedException = false;
            using var session = store.OpenSession();
            try
            {
                var items = session.Query<Record>().Search(x => x.Data, "tester").ToList();
            }
            catch
            {
                //Remove this after search implementation.
                notImplementedException = true;
            }

            Assert.True(notImplementedException);
        }
        {
            using var session = store.OpenSession();
            var raw = session.Advanced.RawQuery<Record>("from index 'Auto/Records/BySearch(Data)' where 'search(Data)' = 'test'").ToList();
            Assert.NotNull(raw);
            Assert.NotEqual(0, raw.Count);
            Assert.Equal(valuesToStore[1], raw.First().Data);
        }
    }

    private class Record
    {
        public string Data { get; init; }
    }
}
