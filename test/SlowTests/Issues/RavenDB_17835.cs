using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17835 : RavenTestBase
{
    public RavenDB_17835(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CachingLayerDoesntThrowNREOnSpecificMethod()
    {
        const int terms = 29;
        using var luceneStore = GetDocumentStore();
        List<Result> results;
        {
            using var luceneSession = luceneStore.BulkInsert();
            results = Enumerable.Range(0, 29).Select(i => new Result() { Age = i % terms, Height = i }).ToList();
            results.ForEach((x) =>
            {
                luceneSession.Store(x);
            });
        }

        {
            var rawQuery = new StringBuilder();
            rawQuery.Append("from Results where boost(Age == 0, 0)");
            for (int i = 1; i < terms; ++i)
                rawQuery.Append($" or boost(Age == {i},{i})");
            rawQuery.Append(" order by score()");

            Assertion(rawQuery.ToString());
        }

        void Assertion(string rawQuery)
        {
            using var luceneSession = luceneStore.OpenSession();
            var luceneResult = luceneSession.Advanced.RawQuery<Result>(rawQuery.ToString()).ToList();
        }
    }

    private class Result
    {
        public int Age { get; set; }
        public int Height { get; set; }
    }
}
