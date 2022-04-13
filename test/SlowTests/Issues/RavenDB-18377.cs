using System.Collections.Generic;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18377 : RavenTestBase
{
    public RavenDB_18377(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanQueryOnNestedEnumerable(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var s = store.OpenSession();
            s.Store(new TestData {Items = new Dictionary<string, Items>() {{"test", new() {Data = new[] {"test", "test", "sad"}}}}});
            s.SaveChanges();
        }

        {
            using var s = store.OpenSession();
            var result = s.Advanced.RawQuery<TestData>("from \"TestDatas\" where Items[].test[].Data[] = 'test'").ToList();
            Assert.NotEmpty(result);
            Assert.Equal(1, result.Count);
            WaitForUserToContinueTheTest(store);
        }
    }

    private class TestData
    {
        public Dictionary<string, Items> Items { get; set; }
    }

    private class Items
    {
        public string[] Data { get; set; }
    }
}
