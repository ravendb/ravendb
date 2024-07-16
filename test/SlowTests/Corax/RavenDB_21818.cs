using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_21818 : RavenTestBase
{
    public RavenDB_21818(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(OrderingType.AlphaNumeric)]
    [InlineData(OrderingType.String)]
    public void ScoreAsSecondaryComparer(OrderingType type)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new Dto(1, new []{1, 1}, "Maciej", 2));
        session.Store(new Dto(1, new []{1, 1, 1}, "Maciej", 1));
        session.SaveChanges();

        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereEquals(nameof(Dto.Numbers), 1)
            .AndAlso()
            .WhereEquals(x => x.Name, "maciej")
            .OrderBy(x => x.SomeNum, type)
            .OrderByScore()
            .ToList();
        
        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].ExpectedOrder);
        Assert.Equal(2, results[1].ExpectedOrder);
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(OrderingType.AlphaNumeric)]
    [InlineData(OrderingType.String)]
    [InlineData(OrderingType.Long)]
    public void ScoreAsBoxedComparer(OrderingType type)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new Dto(1, new []{1, 1}, "Maciej", 2));
        session.Store(new Dto(1, new []{1, 1, 1}, "Maciej", 1));
        session.SaveChanges();

        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereEquals(nameof(Dto.Numbers), 1)
            .AndAlso()
            .WhereEquals(x => x.Name, "maciej")
            .OrderBy(x => x.SomeNum, type)
            .OrderBy(x => x.Name)
            .OrderBy(x => x.SomeNum)
            .OrderByScore()
            .ToList();
        
        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].ExpectedOrder);
        Assert.Equal(2, results[1].ExpectedOrder);
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(OrderingType.AlphaNumeric)]
    [InlineData(OrderingType.String)]
    [InlineData(OrderingType.Long)]
    public void ScoreAsSecondaryComparerWithIndexBoost(OrderingType type)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new Dto(1, new []{1, 1, 1}, "Maciej", 2));
        session.Store(new Dto(1, new []{1, 1}, "Maciej", 1));
        session.SaveChanges();

        var index = new Index();
        index.Execute(store);
        
        var results = session.Advanced.DocumentQuery<Dto>(indexName: index.IndexName)
            .WaitForNonStaleResults()
            .WhereEquals(nameof(Dto.Numbers), 1)
            .AndAlso()
            .WhereEquals(x => x.Name, "maciej")
            .OrderBy(x => x.SomeNum, type)
            .OrderByScore()
            .ToList();
        
        Assert.Equal(2, results.Count);
        //Document boost should boost the document
        Assert.Equal(1, results[0].ExpectedOrder);
        Assert.Equal(2, results[1].ExpectedOrder);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(OrderingType.AlphaNumeric)]
    [InlineData(OrderingType.String)]
    [InlineData(OrderingType.Long)]
    public void ScoreAsSecondaryComparerWithIndexBoostAndIncludeScore(OrderingType type)
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
        };
        
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Dto(1, new[] { 1, 1, 1 }, "Maciej", 2));
            session.Store(new Dto(1, new[] { 1, 1 }, "Maciej", 1));
            session.SaveChanges();
        }

        var index = new Index();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        Span<float> scores = stackalloc float[2];

        {
            using var session = store.OpenSession();
            var query = session.Advanced.DocumentQuery<Dto>(indexName: index.IndexName)
                .WhereEquals(nameof(Dto.Numbers), 1)
                .AndAlso()
                .WhereEquals(x => x.Name, "maciej")
                .OrderByScore();
            using IEnumerator<StreamResult<Dto>> streamResults = session.Advanced.Stream(query, out _);
            
            Assert.True(streamResults.MoveNext());
            Assert.NotNull(streamResults.Current);
            Assert.NotNull(streamResults.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
            scores[0] = (float)streamResults.Current.Metadata.GetDouble(Constants.Documents.Metadata.IndexScore);
           
            Assert.True(streamResults.MoveNext());
            Assert.NotNull(streamResults.Current);
            Assert.NotNull(streamResults.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
            scores[1] = (float)streamResults.Current.Metadata.GetDouble(Constants.Documents.Metadata.IndexScore);
        }



        {
            using var session = store.OpenSession();
            var query = session.Advanced.DocumentQuery<Dto>(indexName: index.IndexName)
                .WhereEquals(nameof(Dto.Numbers), 1)
                .AndAlso()
                .WhereEquals(x => x.Name, "maciej")
                .OrderBy(x => x.SomeNum, type)
                .OrderByScore();



            using IEnumerator<StreamResult<Dto>> streamResults = session.Advanced.Stream(query, out _);
            Assert.True(streamResults.MoveNext());
            Assert.NotNull(streamResults.Current);
            Assert.NotNull(streamResults.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
            Assert.Equal(1, streamResults.Current.Document.ExpectedOrder);
            Assert.Equal(scores[0],(float)streamResults.Current.Metadata.GetDouble(Constants.Documents.Metadata.IndexScore), float.Epsilon);
            
            Assert.True(streamResults.MoveNext());
            Assert.NotNull(streamResults.Current);
            Assert.NotNull(streamResults.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
            Assert.Equal(2, streamResults.Current.Document.ExpectedOrder);
            Assert.Equal(scores[1],(float)streamResults.Current.Metadata.GetDouble(Constants.Documents.Metadata.IndexScore), float.Epsilon);
            
            Assert.False(streamResults.MoveNext());
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(OrderingType.AlphaNumeric)]
    [InlineData(OrderingType.String)]
    [InlineData(OrderingType.Long)]
    public void ScoreAsSecondaryComparerWithIndexBoostAndIncludeScorePaging(OrderingType type)
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
        };
        using var store = GetDocumentStore(options);
        var rnd = new Random(337);
        
        {
            using var bulkInsert = store.BulkInsert();
            for (int i = 0; i < 5000; ++i)
                bulkInsert.Store(new Dto(1, Enumerable.Range(0, rnd.Next(2, 36)).Select(_ => 1).ToArray(), "Maciej", 0));
        }

        var index = new Index();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        
        {
            using var session = store.OpenSession();
            var query = session.Advanced.DocumentQuery<Dto>(indexName: index.IndexName)
                .WhereEquals(nameof(Dto.Numbers), 1)
                .AndAlso()
                .WhereEquals(x => x.Name, "maciej")
                .OrderBy(x => x.SomeNum, type)
                .OrderByScore();
            using IEnumerator<StreamResult<Dto>> streamResults = session.Advanced.Stream(query, out _);
            while (streamResults.MoveNext())
            {
                Assert.NotNull(streamResults.Current);
                Assert.NotNull(streamResults.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
            }
        }
    }
    
    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => from doc in dtos
                let boostVal = doc.ExpectedOrder == 1 ? 1_000_000 : doc.ExpectedOrder
                select new { doc.Numbers, doc.Name, doc.SomeNum }.Boost(boostVal); 
        }
    }

    private record Dto(int SomeNum, int[] Numbers, string Name, int ExpectedOrder, string Id = null);
}
