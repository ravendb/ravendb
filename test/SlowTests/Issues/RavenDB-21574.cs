using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21574 : RavenTestBase
{
    public RavenDB_21574(ITestOutputHelper output) : base(output)
    {
    }

    private class CompanyIndex : AbstractIndexCreationTask<Company>
    {
        public CompanyIndex()
        {
            Map = companies => from company in companies
                select new
                {
                    company.Name
                };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class CompanyIndex_MultiMap : AbstractMultiMapIndexCreationTask<CompanyIndex_MultiMap.Result>
    {
        public class Result
        {
            public string Name { get; set; }
        }

        public CompanyIndex_MultiMap()
        {
            AddMap<Company>(companies => from company in companies
                select new
                {
                    company.Name
                });

            Reduce = results => from result in results
                group result by result.Name
                into g
                select new
                {
                    Name = g.Key
                };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class CompanyIndex_JavaScript : AbstractJavaScriptIndexCreationTask
    {
        public override string IndexName => "Companies/JavaScript";

        public CompanyIndex_JavaScript()
        {
            Maps = new HashSet<string>
            {
                "map('Companies', function(company) { return { Name: company.Name } });"
            };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task Setting_Index_SearchEngineType_Should_Work()
    {
        using (var store = GetDocumentStore())
        {
            await ValidateSearchEngineType<CompanyIndex>(store);
            await ValidateSearchEngineType<CompanyIndex_MultiMap>(store);
            await ValidateSearchEngineType<CompanyIndex_JavaScript>(store);
        }
    }

    private async Task ValidateSearchEngineType<T>(DocumentStore store) where T : AbstractIndexCreationTask, new()
    {
        var index = new T();
        await IndexCreation.CreateIndexesAsync(new List<AbstractIndexCreationTask> { index }, store);
        Indexes.WaitForIndexing(store);

        var indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(index.IndexName));
        var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

        Assert.Equal(nameof(SearchEngineType.Corax), indexDefinition.Configuration[Constants.Configuration.Indexes.IndexingStaticSearchEngineType]);
        Assert.Equal(SearchEngineType.Corax, indexStats.SearchEngineType);

        index.SearchEngineType = SearchEngineType.Lucene;

        await IndexCreation.CreateIndexesAsync(new List<AbstractIndexCreationTask> { index }, store);
        Indexes.WaitForIndexing(store);

        indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(index.IndexName));
        indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

        Assert.Equal(nameof(SearchEngineType.Lucene), indexDefinition.Configuration[Constants.Configuration.Indexes.IndexingStaticSearchEngineType]);
        Assert.Equal(SearchEngineType.Lucene, indexStats.SearchEngineType);

        index.SearchEngineType = SearchEngineType.Corax;
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(index.IndexName));
        indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

        Assert.Equal(nameof(SearchEngineType.Corax), indexDefinition.Configuration[Constants.Configuration.Indexes.IndexingStaticSearchEngineType]);
        Assert.Equal(SearchEngineType.Corax, indexStats.SearchEngineType);

        index.SearchEngineType = SearchEngineType.Lucene;
        store.ExecuteIndex(index);
        Indexes.WaitForIndexing(store);

        indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(index.IndexName));
        indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

        Assert.Equal(nameof(SearchEngineType.Lucene), indexDefinition.Configuration[Constants.Configuration.Indexes.IndexingStaticSearchEngineType]);
        Assert.Equal(SearchEngineType.Lucene, indexStats.SearchEngineType);

        index = new T();
        indexDefinition = index.CreateIndexDefinition();
        await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));
        Indexes.WaitForIndexing(store);

        indexDefinition = await store.Maintenance.SendAsync(new GetIndexOperation(index.IndexName));
        indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

        Assert.Equal(nameof(SearchEngineType.Corax), indexDefinition.Configuration[Constants.Configuration.Indexes.IndexingStaticSearchEngineType]);
        Assert.Equal(SearchEngineType.Corax, indexStats.SearchEngineType);
    }
}
