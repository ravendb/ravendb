using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24794(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CorrectPaginationWhenStreamingFromStartsWith(Options options)
    {
        const int docAmount = (64 * 1024) + 1;
        using var store = GetDocumentStore(options);
        var ids = Enumerable.Range(0, docAmount * 3).Select(x => $"_{x:D8}").ToArray();
        using (var bulkInsert = store.BulkInsert())
        {
            for (int i = 0; i < docAmount; ++i)
            {
                bulkInsert.Store(new Dto() { GroupingKey = i.ToString(), Names = [ids[i]] });
                bulkInsert.Store(new Dto() { GroupingKey = i.ToString(), Names = [ids[ids.Length - 1 - i]] });
            }
        }

        new ReduceIndex().Execute(store);
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
        using (var session = store.OpenSession())
        {
            session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
            List<Dto> allResults = new();
            List<Dto> pagedResults;
            long totalResults = 0;
            long skippedResults = 0;
            int totalUniqueResults = 0;

            int pageNumber = 0;
            int pageSize = 10000;
            do
            {
                if (pageNumber * pageSize + skippedResults > docAmount)
                    Debugger.Break();
                pagedResults = session.Advanced.DocumentQuery<Dto, ReduceIndex>()
                    .WhereStartsWith("Names", "_")
                    .Skip(pageNumber * pageSize + skippedResults)
                    .Take(pageSize)
                    .Statistics(out QueryStatistics stats)
                    .ToList();

                totalResults = stats.TotalResults; // Number of total matching documents (includes duplicates)
                skippedResults += stats.SkippedResults; // Number of duplicate results that were skipped
                totalUniqueResults += pagedResults.Count; // Number of unique results returned in this server call

                pageNumber++;
                allResults.AddRange(pagedResults);
            } while (pagedResults.Count > 0);

            Assert.Equal(docAmount, allResults.Count);
            Assert.Equal(docAmount, totalResults);
            Assert.Equal(docAmount, totalUniqueResults);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CorrectPaginationWhenStreamingFromStartsWithMap(Options options)
    {
        const int docAmount = (64 * 1024) + 1;
        using var store = GetDocumentStore(options);
        var ids = Enumerable.Range(0, docAmount * 3).Select(x => $"_{x:D8}").ToArray();
        using (var bulkInsert = store.BulkInsert())
        {
            for (int i = 0; i < docAmount; ++i)
            {
                bulkInsert.Store(new Dto() { GroupingKey = i.ToString(), Names = [ids[i],ids[ids.Length - 1 - i]] });
            }
        }
        
        new MapIndex().Execute(store);
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
        using (var session = store.OpenSession())
        {
            session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
            List<Dto> allResults = new();
            List<Dto> pagedResults;
            long totalResults = 0;
            long skippedResults = 0;
            int totalUniqueResults = 0;

            int pageNumber = 0;
            int pageSize = 10000;
            do
            {
                if (pageNumber * pageSize + skippedResults > docAmount)
                    Debugger.Break();
                pagedResults = session.Advanced.DocumentQuery<Dto, MapIndex>()
                    .WhereStartsWith("Names", "_")
                    .Skip(pageNumber * pageSize + skippedResults)
                    .Take(pageSize)
                    .Statistics(out QueryStatistics stats)
                    .ToList();

                totalResults = stats.TotalResults; // Number of total matching documents (includes duplicates)
                skippedResults += stats.SkippedResults; // Number of duplicate results that were skipped
                totalUniqueResults += pagedResults.Count; // Number of unique results returned in this server call

                pageNumber++;
                allResults.AddRange(pagedResults);
            } while (pagedResults.Count > 0);

            Assert.Equal(docAmount, allResults.Count);
            Assert.Equal(docAmount, totalResults);
            Assert.Equal(docAmount, totalUniqueResults);
        }
    }
    
    private class MapIndex : AbstractIndexCreationTask<Dto>
    {
        public MapIndex()
        {
            Map = dtos => from doc in dtos
                select new { doc.Names };
        }
    }
    
    private class Dto
    {
        public string GroupingKey { get; set; }
        public string[] Names { get; set; }
    }


    
    private class ReduceIndex : AbstractIndexCreationTask<Dto, Dto>
    {
        public ReduceIndex()
        {
            Map = dtos => from doc in dtos
                select new Dto() { GroupingKey = doc.GroupingKey, Names = doc.Names };

            Reduce = results => from result in results
                group result by result.GroupingKey
                into g
                select new Dto() { GroupingKey = g.Key, Names = g.SelectMany(x => x.Names).ToArray() };
        }
    }
}
