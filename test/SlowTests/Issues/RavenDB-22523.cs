using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22523 : RavenTestBase
{
    public RavenDB_22523(ITestOutputHelper output) : base(output)
    {
    }


    /**
     * Issue with Corax Index and IDocumentQuery - Excludes All.
     *
     * This test is designed to show an issue with the Statistics TotalResults value
     * when using an Excludes All IDocumentQuery. The expected value should be smaller than
     * the total number of documents in the Collection as well as accurate to the filtered value(s).
     */
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [InlineData(10)]
    [InlineData(int.MaxValue)]
    public void ExcludesAllTest(int take)
    {
        // Tags that will be attached to the documents
        var goldTag = new Tag { Id = "gold_tag", Name = "GOLD" };

        var silverTag = new Tag { Id = "silver_tag", Name = "SILVER" };

        var bronzeTag = new Tag { Id = "bronze_tag", Name = "BRONZE" };

        var tags = new List<Tag> { goldTag, silverTag, bronzeTag };

        // store
        using var store = GetDocumentStore();

        // build Corax and Lucene indexes
        store.ExecuteIndex(new TestDocumentCoraxIndex());
        store.ExecuteIndex(new TestDocumentLuceneIndex());

        // Create Tags Collection
        using (var session = store.OpenSession())
        {
            session.Store(goldTag);
            session.Store(silverTag);
            session.Store(bronzeTag);
            session.SaveChanges();
        }

        // Number of test documents to create
        const int numberOfDocsToCreate = 5000;

        var excludeAllCounts = 0;

        var random = new Random(1);

        // bulk insert fake test data
        using (var bulkInsert = store.BulkInsert())
        {
            for (int i = 0; i < numberOfDocsToCreate; i++)
            {
                var data = new TestDocument() { Account = i.ToString(), Amount = i, Tags = { tags[random.Next(0, 2)] } };

                var hasGold = data.Tags.Any(x => x.Id != null && x.Id.Equals(goldTag.Id));

                // randomly decide whether to add silver w/gold tag
                if (hasGold && random.Next(2) == 0)
                {
                    data.Tags.Add(silverTag);

                    // Record how many documents have both the gold and silver tags
                    excludeAllCounts++;
                }

                bulkInsert.Store(data);
            }
        }

        // Prevent stale indexes
        Indexes.WaitForIndexing(store);

        // Filter out "exclude all" docs that have both the gold and silver tags
        var filterValues = new[] { goldTag.Id, silverTag.Id };

        // Query Corax Index
        var coraxResults = GetResults<TestDocumentCoraxIndex>();

        // Query Lucene Index
        var luceneResults = GetResults<TestDocumentLuceneIndex>();

        // Calculate Expected Total Results
        var expectedTotalResults = numberOfDocsToCreate - excludeAllCounts;

        Assert.Equal(take == int.MaxValue ? expectedTotalResults : take, luceneResults.ActualResultsCount);

        Assert.Equal(take == int.MaxValue ? expectedTotalResults : take, coraxResults.ActualResultsCount);

        // Validate Lucene Index Statistics.TotalResults
        Assert.Equal(expectedTotalResults, luceneResults.TotalResults); // Success

        // Validate Corax Index Statistics.TotalResults
        Assert.Equal(expectedTotalResults, coraxResults.TotalResults); // Fails (seems to be returning the Collection size and not the excluded size)

        (long ActualResultsCount, long TotalResults) GetResults<T>() where T : AbstractIndexCreationTask, new()
        {
            using var session = store.OpenSession();
            var query = session.Advanced
                .DocumentQuery<TestDocument, T>()
                .Include(x => x.TagIds);

            // Excludes All Query
            query = query.AndAlso();
            query = query.OpenSubclause();
            query = query.Not.ContainsAll("TagIds", filterValues);
            query = query.OrElse().Not.WhereExists("TagIds");
            query = query.CloseSubclause();
            query = query.Skip(0).Take(take);
            query = query.Statistics(out var stats);
            
            var results = query.OfType<TestDocument>().ToList();
            return (results.Count, stats.TotalResults);
        }
    }
    
    /**
     * Issue with Corax Index and IDocumentQuery - Excludes Any.
     *
     * This test is designed to show an issue with the Statistics TotalResults value
     * when using an Excludes Any IDocumentQuery. The expected value should be smaller than
     * the total number of documents in the Collection as well as accurate to the filtered value(s).
     */
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [InlineData(5000, 10)]
    [InlineData(5000, int.MaxValue)]
    [InlineData(2000000, 10, Skip = "Original case, manual test only.")]
    public void ExcludesAnyTest(int numberOfDocsToCreate, int take)
    {
        // Tags that will be attached to the documents
        var goldTag = new Tag { Id = Guid.NewGuid().ToString(), Name = "GOLD" };

        var silverTag = new Tag { Id = Guid.NewGuid().ToString(), Name = "SILVER" };

        var bronzeTag = new Tag { Id = Guid.NewGuid().ToString(), Name = "BRONZE" };

        var tags = new List<Tag> { goldTag, silverTag, bronzeTag };

        // store
        using var store = GetDocumentStore();

        // build Corax and Lucene indexes
        store.ExecuteIndex(new TestDocumentCoraxIndex());
        store.ExecuteIndex(new TestDocumentLuceneIndex());

        // Create Tags Collection
        using (var session = store.OpenSession())
        {
            session.Store(goldTag);
            session.Store(silverTag);
            session.Store(bronzeTag);
            session.SaveChanges();
        }

        var excludeAnyCounts = 0;

        var random = new Random(1);

        // bulk insert fake test data
        using (var bulkInsert = store.BulkInsert())
        {
            for (int i = 0; i < numberOfDocsToCreate; i++)
            {
                var data = new TestDocument() { Account = i.ToString(), Amount = i, Tags = { tags[random.Next(0, 2)] } };
                // Record how many documents have the silver tag
                if (data.Tags.Any(x => x.Id != null && x.Id.Equals(silverTag.Id)))
                {
                    excludeAnyCounts++;
                }

                bulkInsert.Store(data);
            }
        }

        // Prevent stale indexes
        Indexes.WaitForIndexing(store);

        // Debug via Raven Studio

        // Filter out "exclude any" docs that have the silver tag
        string[] filterValues = [silverTag.Id];

        // Query Corax Index
        var coraxResults = GetResults<TestDocumentCoraxIndex>();

        // Query Lucene Index
        var luceneResults = GetResults<TestDocumentLuceneIndex>();

        // Calculate Expected Total Results
        var expectedTotalResults = numberOfDocsToCreate - excludeAnyCounts;

        Assert.Equal(take == int.MaxValue ? expectedTotalResults : take, luceneResults.ActualResultsCount);

        Assert.Equal(take == int.MaxValue ? expectedTotalResults : take, coraxResults.ActualResultsCount);

        // Validate Lucene Index Statistics.TotalResults
        Assert.Equal(expectedTotalResults, luceneResults.TotalResults); // Success

        // Validate Corax Index Statistics.TotalResults
        Assert.Equal(expectedTotalResults, coraxResults.TotalResults); // Fails (seems to be returning the Collection size and not the excluded size)

        (long ActualResultsCount, long TotalResults) GetResults<T>() where T : AbstractIndexCreationTask, new()
        {
            using var session = store.OpenSession();
            var query = session.Advanced
                .DocumentQuery<TestDocument, T>()
                .Include(x => x.TagIds);

            // Excludes Any Query
            query = query.AndAlso();
            query = query.OpenSubclause();
            query = query.Not.ContainsAny("TagIds", filterValues);
            query = query.OrElse().Not.WhereExists("TagIds");
            query = query.CloseSubclause();
            query = query.Skip(0).Take(take);
            query = query.Statistics(out var stats);

            var results = query.OfType<TestDocument>().ToList();
            return (results.Count, stats.TotalResults);
        }
    }


    private class Tag
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class TestDocument
    {
        public string Account { get; set; }
        public decimal Amount { get; set; }
        public List<Tag> Tags { get; set; } = [];
        public List<string> TagIds => Tags.Select(x => x.Id).ToList();
    }

    private class TestDocumentCoraxIndex : AbstractIndexCreationTask<TestDocument>
    {
        public TestDocumentCoraxIndex()
        {
            Map = docs => from doc in docs
                select new { doc.Account, doc.Amount, doc.Tags, TagIds = Enumerable.ToArray(doc.Tags.Select(x => x.Id)) };
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class TestDocumentLuceneIndex : AbstractIndexCreationTask<TestDocument>
    {
        public TestDocumentLuceneIndex()
        {
            Map = docs => from doc in docs
                select new { doc.Account, doc.Amount, doc.Tags, TagIds = Enumerable.ToArray(doc.Tags.Select(x => x.Id)) };
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }
}
