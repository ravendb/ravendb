using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Corax.Queries;
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
    private record Dto(string Name, long LongValue, double DoubleValue, string Id = null);


    /*
     * Things to test
     * StartsWith -> ASC [X] DESC[X] MAX_PREFIX[X]
     * EndsWith -> ASC[X]  DESC[X]
     * Regex -> ASC[X]  DESC[X]
     * Exists -> ASC[X]  DESC[X]
     *
     * Range Numeric
     * Less Than L_ASC[X] L_DSC[X] D_ASC[X] D_DSC[X] S_ASC[X] S_DSC[X]
     * Less Than Or Equal L_ASC[X] L_DSC[X] D_ASC[X] L_DSC[X] S_ASC[X] S_DSC[X]
     * Greater Than L_ASC[X] L_DSC[X] D_ASC[X] L_DSC[X] S_ASC[X] S_DSC[X]
     * Greater Than Or Equal L_ASC[X] L_DSC[X] D_ASC[X] L_DSC[X] S_ASC[X] S_DSC[X]
     * Range Queries L_ASC[X]  L_DSC[X] D_ASC[X] D_DSC[X] S_ASC[X] S_DSC[X]
     */

    public static IEnumerable<object[]> UnboundedRange()
    {
        foreach (var unaryOperation in new UnaryMatchOperation[] {UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual})
        foreach (var fieldType in new OrderingType[] {OrderingType.String, OrderingType.Long, OrderingType.Double})
        foreach (var isAscending in new bool[]{true, false})
        {
            object value = fieldType switch
            {
                OrderingType.Double => 2.0D,
                OrderingType.Long => 4L,
                OrderingType.String => "a",
                _ => throw new ArgumentOutOfRangeException()
            };
            
            yield return new object[] {unaryOperation, fieldType, isAscending, value};
        }
    }

    [Theory]
    [MemberData(nameof(UnboundedRange))]
    public void UnboundedRangeQueries(UnaryMatchOperation unaryMatchOperation, OrderingType fieldType, bool ascending, object value)
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();
        var fieldName = fieldType switch
        {
            OrderingType.Double => nameof(Dto.DoubleValue),
            OrderingType.Long => nameof(Dto.LongValue),
            _ => nameof(Dto.Name)
        };

        object queriedValue = null;
        if (fieldType == OrderingType.Long)
            queriedValue = (long)value;
        if (fieldType == OrderingType.Double)
            queriedValue = (double)value;
        if (fieldType is not (OrderingType.Double or OrderingType.Long))
            queriedValue = (string)value;

        var query = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults();

        query = unaryMatchOperation switch
        {
            UnaryMatchOperation.LessThan when queriedValue is long l => query.WhereLessThan(fieldName, l),
            UnaryMatchOperation.LessThan when queriedValue is double d => query.WhereLessThan(fieldName, d),
            UnaryMatchOperation.LessThan when queriedValue is string s => query.WhereLessThan(fieldName, s),

            UnaryMatchOperation.LessThanOrEqual when queriedValue is long l => query.WhereLessThanOrEqual(fieldName, l),
            UnaryMatchOperation.LessThanOrEqual when queriedValue is double d => query.WhereLessThanOrEqual(fieldName, d),
            UnaryMatchOperation.LessThanOrEqual when queriedValue is string s => query.WhereLessThanOrEqual(fieldName, s),

            UnaryMatchOperation.GreaterThan when queriedValue is long l => query.WhereGreaterThan(fieldName, l),
            UnaryMatchOperation.GreaterThan when queriedValue is double d => query.WhereGreaterThan(fieldName, d),
            UnaryMatchOperation.GreaterThan when queriedValue is string s => query.WhereGreaterThan(fieldName, s),

            UnaryMatchOperation.GreaterThanOrEqual when queriedValue is long l => query.WhereGreaterThanOrEqual(fieldName, l),
            UnaryMatchOperation.GreaterThanOrEqual when queriedValue is double d => query.WhereGreaterThanOrEqual(fieldName, d),
            UnaryMatchOperation.GreaterThanOrEqual when queriedValue is string s => query.WhereGreaterThanOrEqual(fieldName, s),

            _ => throw new InvalidDataException(unaryMatchOperation.ToString())
        };

        query = ascending
            ? query.OrderBy(fieldName, fieldType)
            : query.OrderByDescending(fieldName, fieldType);

        var serverResults = query.ToList();

        Func<Dto, bool> filter = unaryMatchOperation switch
        {
            UnaryMatchOperation.LessThan when queriedValue is long l => dto => dto.LongValue < l,
            UnaryMatchOperation.LessThan when queriedValue is double d => dto => dto.DoubleValue < d,
            UnaryMatchOperation.LessThan when queriedValue is string s => dto => dto.Name.AsSpan().SequenceCompareTo(s) < 0,

            UnaryMatchOperation.LessThanOrEqual when queriedValue is long l => dto => dto.LongValue <= l,
            UnaryMatchOperation.LessThanOrEqual when queriedValue is double d => dto => dto.DoubleValue <= d,
            UnaryMatchOperation.LessThanOrEqual when queriedValue is string s => dto => dto.Name.AsSpan().SequenceCompareTo(s) <= 0,

            UnaryMatchOperation.GreaterThan when queriedValue is long l => dto => dto.LongValue > l,
            UnaryMatchOperation.GreaterThan when queriedValue is double d => dto => dto.DoubleValue > d,
            UnaryMatchOperation.GreaterThan when queriedValue is string s => dto => dto.Name.AsSpan().SequenceCompareTo(s) > 0,

            UnaryMatchOperation.GreaterThanOrEqual when queriedValue is long l => dto => dto.LongValue >= l,
            UnaryMatchOperation.GreaterThanOrEqual when queriedValue is double d => dto => dto.DoubleValue >= d,
            UnaryMatchOperation.GreaterThanOrEqual when queriedValue is string s => dto => dto.Name.AsSpan().SequenceCompareTo(s) >= 0,

            _ => throw new InvalidDataException(unaryMatchOperation.ToString())
        };
        
        var linqResults = actualDocuments.Where(filter);
        linqResults = (ascending, fieldType) switch
        {
            (true, OrderingType.String) => linqResults.OrderBy(i => i.Name),
            (true, OrderingType.Long) => linqResults.OrderBy(i => i.LongValue),
            (true, OrderingType.Double) => linqResults.OrderBy(i => i.DoubleValue),
            (false, OrderingType.String) => linqResults.OrderByDescending(i => i.Name),
            (false, OrderingType.Long) => linqResults.OrderByDescending(i => i.LongValue),
            (false, OrderingType.Double) => linqResults.OrderByDescending(i => i.DoubleValue),
            _ => throw new ArgumentOutOfRangeException()
        };
        Assert.Equal(linqResults.Select(i => i.Id), serverResults.Select(i => i.Id));
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
    public void DescendingEndsWithWithStreamingReturnsGoodOrder()
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

  

    [RavenTheory(RavenTestCategory.Querying)]
    [MemberData(nameof(RangesTests))]
    public void RangeTests(bool leftInclusive, bool rightInclusive, bool ascending, OrderingType fieldType)
    {
        using var store = CreateDatabase(out List<Dto> actualDocuments);
        using var session = store.OpenSession();

        var query = session.Advanced.DocumentQuery<Dto>()
            .WaitForNonStaleResults();

        string fieldName = fieldType switch
        {
            OrderingType.Long => nameof(Dto.LongValue),
            OrderingType.Double => nameof(Dto.DoubleValue),
            _ => nameof(Dto.Name)
        };
        
        object leftValue;
        if (fieldType == OrderingType.Long)
            leftValue = -4L;
        else if (fieldType == OrderingType.Double)
            leftValue = -2.5D;
        else
            leftValue = "a";
        
        object rightValue;
        if (fieldType == OrderingType.Long)
            rightValue = 4L;
        else if (fieldType == OrderingType.Double)
            rightValue = 2.0D;
        else
            rightValue = Encodings.Utf8.GetString(new byte[] {255});

        query = leftInclusive 
            ? query.WhereGreaterThanOrEqual(fieldName, leftValue) 
            : query.WhereGreaterThan(fieldName, leftValue);
        
        query = rightInclusive 
            ? query.WhereLessThanOrEqual(fieldName, rightValue) 
            : query.WhereLessThan(fieldName, rightValue);
        
        query = (ascending, fieldType) switch
        {
            (true, OrderingType.Long) => query.OrderBy(fieldName, OrderingType.Long),
            (true, OrderingType.Double) => query.OrderBy(fieldName, OrderingType.Double),
            (true, OrderingType.String) => query.OrderBy(fieldName, OrderingType.String),
            
            (false, OrderingType.Long) => query.OrderByDescending(fieldName, OrderingType.Long),
            (false, OrderingType.Double) => query.OrderByDescending(fieldName, OrderingType.Double),
            (false, OrderingType.String) => query.OrderByDescending(fieldName, OrderingType.String),
            _ => throw new ArgumentOutOfRangeException()
        };

        Func<Dto, bool> leftSideQuery = (leftInclusive, fieldType) switch
        {
            (true, OrderingType.Long) => dto => dto.LongValue >= (long)leftValue,
            (false, OrderingType.Long) => dto => dto.LongValue > (long)leftValue,
            
            (true, OrderingType.Double) => dto => dto.DoubleValue >= (double)leftValue,
            (false, OrderingType.Double) => dto => dto.DoubleValue > (double)leftValue,
            
            (true, OrderingType.String) => dto => dto.Name.AsSpan().SequenceCompareTo((string)leftValue) >= 0,
            (false, OrderingType.String) => dto => dto.Name.AsSpan().SequenceCompareTo((string)leftValue) > 0,
            _ => throw new ArgumentOutOfRangeException()
        };
        
        Func<Dto, bool> rightSideQuery = (rightInclusive, fieldType) switch
        {
            (true, OrderingType.Long) => dto => dto.LongValue <= (long)rightValue,
            (false, OrderingType.Long) => dto => dto.LongValue < (long)rightValue,
            (true, OrderingType.Double) => dto => dto.DoubleValue <= (double)rightValue,
            (false, OrderingType.Double) => dto => dto.DoubleValue < (double)rightValue,
            
            (true, OrderingType.String) => dto => dto.Name.AsSpan().SequenceCompareTo((string)rightValue) <= 0,
            (false, OrderingType.String) => dto => dto.Name.AsSpan().SequenceCompareTo((string)rightValue) < 0,
            _ => throw new ArgumentOutOfRangeException()
        };

        var queryResults = query.ToList();
        var actualDocsResults = actualDocuments.Where(leftSideQuery).Where(rightSideQuery);
        var actualDocsSortedResults = ((ascending, fieldType) switch
        {
            (true, OrderingType.Long) => actualDocsResults.OrderBy(i => i.LongValue),
            (false, OrderingType.Long) => actualDocsResults.OrderByDescending(i => i.LongValue),
            (true, OrderingType.Double) => actualDocsResults.OrderBy(i => i.DoubleValue),
            (false, OrderingType.Double) => actualDocsResults.OrderByDescending(i => i.DoubleValue),
            (true, OrderingType.String) => actualDocsResults.OrderBy(i => i.Name),
            (false, OrderingType.String) => actualDocsResults.OrderByDescending(i => i.Name),
            _ => throw new ArgumentOutOfRangeException()
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
        foreach (var fieldType in new[] {OrderingType.Long, OrderingType.String, OrderingType.Double})
        {
            yield return new object[] {leftInclusive, rightInclusive, isAscending, fieldType};
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
    
}
