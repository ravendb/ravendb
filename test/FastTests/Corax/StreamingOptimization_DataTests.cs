using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class StreamingOptimization_DataTests : RavenTestBase
{
    public StreamingOptimization_DataTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void AscendingStartsWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereStartsWith(i => i.Name, "a")
            .OrderBy(i => i.Name)
            .ToList();
        
        Assert.Equal(actualDocuments.Where(i =>i.Name.StartsWith("a")).OrderBy(i => i.Name).Select(i => i.Id), results.Select(i => i.Id));
    }
    
    [Fact]
    public void DescendingStartsWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereStartsWith(i => i.Name, "a")
            .OrderByDescending(i => i.Name)
            .ToList();
        
        Assert.Equal(actualDocuments.Where(i =>i.Name.StartsWith("a")).OrderByDescending(i => i.Name).Select(i => i.Id), results.Select(i => i.Id));
    }

    [Fact]
    public void DescendingMaximumPrefixStartsWithStreamingReturnsGoodOrder()
    {
        string maxPrefix = Encodings.Utf8.GetString(new byte[] {255, 255});
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereStartsWith(i => i.Name, maxPrefix)
            .OrderByDescending(i => i.Name)
            .ToList();
        
        Assert.Equal(actualDocuments.Where(i =>i.Name.StartsWith(maxPrefix)).OrderByDescending(i => i.Name).Select(i => i.Id), results.Select(i => i.Id));
        
    }
    
    [Fact]
    public void EndsWithWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereEndsWith(i => i.Name, "a")
            .OrderBy(i => i.Name)
            .ToList();
        
        Assert.Equal(actualDocuments.Where(i =>i.Name.EndsWith("a")).OrderBy(i => i.Name).Select(i => i.Id), results.Select(i => i.Id));
    }

    [Fact]
    public void EncodingEndsWithWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereEndsWith(i => i.Name, "a")
            .OrderByDescending(i => i.Name)
            .ToList();
        
        Assert.Equal(actualDocuments.Where(i =>i.Name.EndsWith("a")).OrderByDescending(i => i.Name).Select(i => i.Id), results.Select(i => i.Id));
    }
    
    [Fact]
    public void ExistsWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereExists(i => i.Name)
            .OrderBy(i => i.Name)
            .ToList();
        
        Assert.Equal(actualDocuments.OrderBy(i => i.Name).Select(i => i.Id), results.Select(i => i.Id));
    }

    [Fact]
    public void DescendingExistsWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereExists(i => i.Name)
            .OrderByDescending(i => i.Name)
            .ToList();
        
        Assert.Equal(actualDocuments.OrderByDescending(i => i.Name).Select(i => i.Id), results.Select(i => i.Id));
    }
    
    [Fact]
    public void RegexWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereRegex(i => i.Name, ".*?a.*") //basically contains 'a'
            .OrderBy(i => i.Name)
            .ToList();
        
        Assert.Equal(actualDocuments.Where(i=> i.Name.Contains("a")).OrderBy(i => i.Name).Select(i => i.Id), results.Select(i => i.Id));
    }

    [Fact]
    public void DescendingRegexWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereRegex(i => i.Name, ".*?a.*")
            .OrderByDescending(i => i.Name)
            .ToList();
        
        Assert.Equal(actualDocuments.Where(i=> i.Name.Contains("a")).OrderByDescending(i => i.Name).Select(i => i.Id), results.Select(i => i.Id));
    }

    [Fact]
    public void LessThanLongsWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereLessThan(i => i.LongValue, 4)
            .OrderBy(i => i.LongValue, OrderingType.Long)
            .ToList();

        Assert.Equal(actualDocuments.Where(i=> i.LongValue < 4).OrderBy(i => i.LongValue).Select(i => i.Id), results.Select(i => i.Id));
    }
    
    [Fact]
    public void DescendingLessThanLongsWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereLessThan(i => i.LongValue, 4)
            .OrderByDescending(i => i.LongValue, OrderingType.Long)
            .ToList();

        Assert.Equal(actualDocuments.Where(i=> i.LongValue < 4).OrderByDescending(i => i.LongValue).Select(i => i.Id), results.Select(i => i.Id));
    }
    
    [Fact]
    public void LessThanDoublesWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereLessThan(i => i.DoubleValue, 4.0)
            .OrderBy(i => i.DoubleValue, OrderingType.Double)
            .ToList();

        Assert.Equal(actualDocuments.Where(i=> i.DoubleValue < 4.0).OrderBy(i => i.DoubleValue).Select(i => i.Id), results.Select(i => i.Id));
    }
    
    [Fact]
    public void DescendingLessThanDoublesWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereLessThan(i => i.DoubleValue, 4.0)
            .OrderByDescending(i => i.DoubleValue, OrderingType.Double)
            .ToList();

        Assert.Equal(actualDocuments.Where(i=> i.DoubleValue < 4.0).OrderByDescending(i => i.DoubleValue).Select(i => i.Id), results.Select(i => i.Id));
    }
    
    [Fact]
    public void LessThanOrEqualLongsWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereLessThanOrEqual(i => i.LongValue, 4)
            .OrderBy(i => i.LongValue, OrderingType.Long)
            .ToList();

        Assert.Equal(actualDocuments.Where(i=> i.LongValue <= 4).OrderBy(i => i.LongValue).Select(i => i.Id), results.Select(i => i.Id));
    }
    
    [Fact]
    public void DescendingLessThanOrEqualLongsWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereLessThanOrEqual(i => i.LongValue, 4)
            .OrderByDescending(i => i.LongValue, OrderingType.Long)
            .ToList();

        Assert.Equal(actualDocuments.Where(i=> i.LongValue <= 4).OrderByDescending(i => i.LongValue).Select(i => i.Id), results.Select(i => i.Id));
    }
    
    [Fact]
    public void LessThanOrEqualDoublesWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereLessThanOrEqual(i => i.DoubleValue, 4.0)
            .OrderBy(i => i.DoubleValue, OrderingType.Double)
            .ToList();

        Assert.Equal(actualDocuments.Where(i=> i.DoubleValue <= 4.0).OrderBy(i => i.LongValue).Select(i => i.Id), results.Select(i => i.Id));
    }
    
    [Fact]
    public void DescendingLessThanOrEqualDoublesWithStreamingReturnsGoodOrder()
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults()
            .WhereLessThanOrEqual(i => i.DoubleValue, 4.0)
            .OrderByDescending(i => i.DoubleValue, OrderingType.Double)
            .ToList();

        Assert.Equal(actualDocuments.Where(i=> i.DoubleValue <= 4.0).OrderByDescending(i => i.DoubleValue).Select(i => i.Id), results.Select(i => i.Id));
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [MemberData(nameof(RangesTests))]
    public void RangeTests(bool leftInclusive, bool rightInclusive, bool ascending, bool isLong)
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();

        var query = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults();

        string fieldName = isLong ? nameof(Dto.LongValue) : nameof(Dto.DoubleValue);
        
        object leftValue;
        if (isLong)
            leftValue = -4L;
        else
            leftValue = -2.5D;
        
        object rightValue;
        if (isLong)
            rightValue = 4L;
        else
            rightValue = 2.0D;

        query = leftInclusive 
            ? query.WhereGreaterThanOrEqual(fieldName, leftValue) 
            : query.WhereGreaterThan(fieldName, leftValue);
        
        query = rightInclusive 
            ? query.WhereLessThanOrEqual(fieldName, rightValue) 
            : query.WhereLessThan(fieldName, rightValue);
        
        query = (ascending, isLong) switch
        {
            (true, true) => query.OrderBy(fieldName, OrderingType.Long),
            (true, false) => query.OrderBy(fieldName, OrderingType.Double),
            (false, true) => query.OrderByDescending(fieldName, OrderingType.Long),
            (false, false) => query.OrderByDescending(fieldName, OrderingType.Double)
        };

        Func<Dto, bool> leftSideQuery = (leftInclusive, isLong) switch
        {
            (true, true) => dto => dto.LongValue >= (long)leftValue,
            (false, true) => dto => dto.LongValue > (long)leftValue,
            (true, false) => dto => dto.DoubleValue >= (double)leftValue,
            (false, false) => dto => dto.DoubleValue > (double)leftValue,
        };
        
        Func<Dto, bool> rightSideQuery = (rightInclusive, isLong) switch
        {
            (true, true) => dto => dto.LongValue <= (long)rightValue,
            (false, true) => dto => dto.LongValue < (long)rightValue,
            (true, false) => dto => dto.DoubleValue <= (double)rightValue,
            (false, false) => dto => dto.DoubleValue < (double)rightValue,
        };

        var queryResults = query.ToList();
        var actualDocsResults = actualDocuments.Where(leftSideQuery).Where(rightSideQuery);
        var actualDocsSortedResults = ((ascending, isLong) switch
        {
            (true, true) => actualDocsResults.OrderBy(i => i.LongValue),
            (false, true) => actualDocsResults.OrderByDescending(i => i.LongValue),
            (true, false) => actualDocsResults.OrderBy(i => i.DoubleValue),
            (false, false) => actualDocsResults.OrderByDescending(i => i.DoubleValue),
        }).ToList();
        
        //Assert results
        
        Assert.Equal(actualDocsSortedResults.Select(i=> i.Id), queryResults.Select(i => i.Id));
    }
    
    
    public static IEnumerable<object[]> RangesTests()
    {
        var boolean = new bool[] {false, true};
        foreach (var leftInclusive in boolean)
        foreach (var rightInclusive in boolean)
        foreach (var isAscending in boolean)
        foreach (var isLong in boolean)
        {
            yield return new object[] {leftInclusive, rightInclusive, isAscending, isLong};
        }
    }
    
    
    private IDocumentStore CreateDatabase(out List<Dto> documents)
    {
        var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        documents = new();
        documents.Add(new Dto("aaaaa", -5, -2.5));
        documents.Add(new Dto("aaaab", -4, -2.0));
        documents.Add(new Dto("abaaa", -3, -1.5));
        documents.Add(new Dto("abaab", -2, -1.0));
        documents.Add(new Dto("acaaa", -1, -0.5));
        documents.Add(new Dto("acaab", -0, 0.0));
        documents.Add(new Dto("aeaaa", 1, 0.5));
        documents.Add(new Dto("aeaab", 2, 1.0));
        documents.Add(new Dto("afaaa", 3, 1.5));
        documents.Add(new Dto("afaab", 4, 2.0));
        documents.Add(new Dto("agaaa", 5, 2.5));
        
        //We need to test condition for maximum prefixes
        documents.Add(new Dto(Encodings.Utf8.GetString(new byte[]{255, 255, 255}), int.MaxValue ,float.MaxValue));
        documents.Add(new Dto(Encodings.Utf8.GetString(new byte[]{255, 255, 254}), int.MaxValue ,float.MaxValue));
        documents.Add(new Dto(Encodings.Utf8.GetString(new byte[]{255, 255, 253}), int.MaxValue ,float.MaxValue));

        documents.Add(new Dto("bbbbbbbbb", long.MaxValue, double.MaxValue));
        Shuffle(documents, 1241323);

        using (var session = store.OpenSession())
        {
            foreach (var dto in documents)
                session.Store(dto);
            session.SaveChanges();

            //ensure 
            var list = session.Query<Dto>().ToList();
            Assert.NotEqual(list.OrderBy(i=> i.Id).Select(i => i.Id), list.Select(i => i.Id));
        }
        
        
        
        return store;
    }

    private static void Shuffle<T>(IList<T> list, int seed)
    {
        var random = new Random(seed);
        var n = list.Count;
        while (n > 1)
        {
            n--;
            var k = random.Next(n + 1);
            (list[k], list[n]) = (list[n], list[k]);
        }
    }
    
    private record Dto(string Name, long LongValue, double DoubleValue, string Id = null);
}
