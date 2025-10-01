using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22504 : RavenTestBase
{
    public RavenDB_22504(ITestOutputHelper output) : base(output)
    {
    }
    
    private class DocumentClass
    {
        public string Id { get; set; } = null!;
    }

    private void Test(int numberOfDocuments, Options options)
    {
        var random = new Random(2222);

        var unsortedIds = new List<string>();
        
        using var store = GetDocumentStore(options);
        using (var bulkInsert = store.BulkInsert())
        {
            for (int i = 0; i < numberOfDocuments; i++)
            {
                var id = $"fields-{random.Next(numberOfDocuments)}-A";
                bulkInsert.Store(new DocumentClass { Id = id });
                
                if (unsortedIds.Contains(id) == false)
                    unsortedIds.Add(id);
            }
        }

        var documentsById = store.OpenSession().Query<DocumentClass>().Statistics(out var stats).Customize(x => x.WaitForNonStaleResults())
            .OrderBy(cr => cr.Id, OrderingType.AlphaNumeric).Select(cr => cr.Id).ToArray();
        Assert.False(stats.IsStale);
        Assert.Equal(documentsById.Length, unsortedIds.Count);
        
        for (int i = 0; i < documentsById.Length - 1; i++)
        {
            var document1 = documentsById[i];
            var document2 = documentsById[i + 1];
            Assert.True(int.Parse(document1[7..][..^2]) <= int.Parse(document2[7..][..^2]), $"{int.Parse(document1[7..][..^2])} <= {int.Parse(document2[7..][..^2])} OR {document1} <= {document2}");
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(99)]
    [InlineData(101)]
    [InlineData(1001)]
    [InlineData(9685)]
    [InlineData(17528)]
    [InlineData(45777)]
    //This sometimes doesn't work as expected
    public void TestAlphaNumericOrderByCorax(int numberOfDocuments) => Test(numberOfDocuments, Options.ForSearchEngine(RavenSearchEngineMode.Corax));

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(99)]
    [InlineData(101)]
    [InlineData(1001)]
    [InlineData(9685)]
    [InlineData(17528)]
    [InlineData(45777)]
    public void TestAlphaNumericOrderByLucene(int numberOfDocuments) => Test(numberOfDocuments, Options.ForSearchEngine(RavenSearchEngineMode.Lucene));
}
