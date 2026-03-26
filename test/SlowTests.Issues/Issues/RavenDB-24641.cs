using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24641 : RavenTestBase
{
    public RavenDB_24641(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Core | RavenTestCategory.Indexes)]
    public void DatabaseCompactOperationWillSkipCoraxIndexes()
    {
        using var store = GetDocumentStore(new Options() { RunInMemory = false });
        using (var session = store.OpenSession())
        {
            session.Store(new Orders.Order { Company = "Company1" });
            session.SaveChanges();
        }

        var coraxIndex = new IndexToCompact(SearchEngineType.Corax, "CoraxIndex");
        var luceneIndex = new IndexToCompact(SearchEngineType.Lucene, "LuceneIndex");
        coraxIndex.Execute(store);
        luceneIndex.Execute(store);

        Indexes.WaitForIndexing(store);


        var compactDatabaseOperation = new CompactDatabaseOperation(new CompactSettings()
        {
            DatabaseName = store.Database, Documents = true, Indexes = [coraxIndex.IndexName, luceneIndex.IndexName], SkipOptimizeIndexes = false
        });

        var operation = store.Maintenance.Server.Send(compactDatabaseOperation);
        var result = operation.WaitForCompletion(TimeSpan.FromSeconds(30));
        var compactionResult = Assert.IsType<CompactionResult>(result);
        Assert.Equal(2, compactionResult.IndexesResults.Count);
        var coraxResult = compactionResult.IndexesResults[coraxIndex.IndexName];
        Assert.True(coraxResult.Processed);
        Assert.True(coraxResult.Skipped);
        Assert.Contains("Skipping data compaction of 'CoraxIndex' index because data compaction of Corax indexes isn't supported.", coraxResult.Message);
        
        var luceneResult = compactionResult.IndexesResults[luceneIndex.IndexName];
        Assert.True(luceneResult.Processed);
        Assert.False(luceneResult.Skipped);

        Assert.True(compactionResult.Processed);
        Assert.False(compactionResult.Skipped);
    }

    private class IndexToCompact : AbstractIndexCreationTask<Orders.Order>
    {
        private string _indexName;
        public override string IndexName => _indexName;

        public IndexToCompact(SearchEngineType searchEngineType, string indexName)
        {
            Map = orders => from order in orders
                select new { order.Company };

            SearchEngineType = searchEngineType;
            _indexName = indexName;
        }
    }
}
