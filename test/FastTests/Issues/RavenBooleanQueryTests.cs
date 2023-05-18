using System.Linq;
using Lucene.Net.Search;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;
using Query = Lucene.Net.Search.Query;

namespace FastTests.Issues;

//Low-level query optimization tests. 
public class RavenBooleanQueryTests : RavenTestBase
{
    [Theory]
    [InlineData(OperatorType.And, Occur.MUST)]
    [InlineData(OperatorType.Or, Occur.SHOULD)]
    public void CanMergeTwoRavenBooleanQueryWithoutBoosting(OperatorType operatorType, Occur occur)
    {
        var rbqLeft = new RavenBooleanQuery(operatorType) {{GetExampleTerm("1"), occur}, {GetExampleTerm("2"), occur}};

        var rbqRight = new RavenBooleanQuery(operatorType) {{GetExampleTerm("3"), occur}, {GetExampleTerm("4"), occur}};

        Assert.True(operatorType is OperatorType.And
            ? rbqLeft.TryAnd(rbqRight, null)
            : rbqLeft.TryOr(rbqRight, null));

        var merged = rbqLeft;
        Assert.Equal(4, merged.Clauses.Count);
        for (int i = 0; i < 4; ++i)
        {
            var clause = merged.Clauses[i].Query as TermQuery;
            Assert.NotNull(clause);
            Assert.Equal($"{i + 1}", clause.Term.Text);
        }
    }

    [Theory]
    [InlineData(OperatorType.And, Occur.MUST)]
    [InlineData(OperatorType.Or, Occur.SHOULD)]
    public void ShouldNotMergeTwoDifferentBoosting(OperatorType operatorType, Occur occur)
    {
        //this is boosted (even it's 0)
        var rbqLeft = new RavenBooleanQuery(operatorType) {{GetExampleTerm("1"), occur}, {GetExampleTerm("2"), occur}};
        rbqLeft.Boost = 0f;

        var third = GetExampleTerm("3");

        var resultOfMerge = operatorType is OperatorType.And
            ? rbqLeft.TryAnd(third, null)
            : rbqLeft.TryOr(third, null);

        var mode = occur is Occur.MUST ? "+" : "";
        Assert.NotEqual($"({mode}exampleField:1 {mode}exampleField:2 {mode}exampleField:3)^.0", rbqLeft.ToString());
        Assert.Equal($"({mode}exampleField:1 {mode}exampleField:2)^.0", rbqLeft.ToString());

        Assert.Equal(false, resultOfMerge);
    }

    [Theory]
    [InlineData(OperatorType.And, Occur.MUST)]
    [InlineData(OperatorType.Or, Occur.SHOULD)]
    public void TwoBoostedRavenBooleanQueriesShouldNotBeMerged(OperatorType operatorType, Occur occur)
    {
        var firstTerm = GetExampleTerm("1");
        var secondTerm = GetExampleTerm("2");
        var rbqLeft = new RavenBooleanQuery(operatorType) {{firstTerm, occur}, {secondTerm, occur}};
        rbqLeft.Boost = 2.0f;
        var rbqRight = new RavenBooleanQuery(operatorType) {{GetExampleTerm("2"), occur}, {GetExampleTerm("3"), occur}};
        rbqRight.Boost = 3.0f;
        
        
        Assert.False(operatorType is OperatorType.And 
            ? rbqLeft.TryAnd(rbqRight, null) 
            : rbqLeft.TryOr(rbqRight, null));

        Assert.Equal(2, rbqLeft.Clauses.Count);
        Assert.Equal(firstTerm, rbqLeft.Clauses[0].Query);
        Assert.Equal(secondTerm, rbqLeft.Clauses[1].Query);
    }
    
    [Theory]
    [InlineData(OperatorType.And, Occur.MUST)]
    [InlineData(OperatorType.Or, Occur.SHOULD)]
    public void CanMergeTwoNotBoostedRbq(OperatorType operatorType, Occur occur)
    {
        var firstTerm = GetExampleTerm("1");
        var secondTerm = GetExampleTerm("2");
        var thirdTerm = GetExampleTerm("3", boost: 10);
        var fourthTerm = GetExampleTerm("4", boost: 15);
        var rbqLeft = new RavenBooleanQuery(operatorType) {{firstTerm, occur}, {secondTerm, occur}};
        var rbqRight = new RavenBooleanQuery(operatorType) {{thirdTerm, occur}, {fourthTerm, occur}};


        Assert.True(operatorType is OperatorType.And 
            ? rbqLeft.TryAnd(rbqRight, null) 
            : rbqLeft.TryOr(rbqRight, null));

        Assert.Equal(4, rbqLeft.Clauses.Count);
        Assert.Equal(firstTerm, rbqLeft.Clauses[0].Query);
        Assert.Equal(secondTerm, rbqLeft.Clauses[1].Query);
        Assert.Equal(thirdTerm, rbqLeft.Clauses[2].Query);
        Assert.True(thirdTerm.Boost.AlmostEquals(10));
        Assert.Equal(fourthTerm, rbqLeft.Clauses[3].Query);
        Assert.True(fourthTerm.Boost.AlmostEquals(15));
    }
    
    [Theory]
    [InlineData(OperatorType.And, Occur.MUST)]
    [InlineData(OperatorType.Or, Occur.SHOULD)]
    public void LeftBoostedRightNotBoostedWillNotBeMerged(OperatorType operatorType, Occur occur)
    {
        var firstTerm = GetExampleTerm("1");
        var secondTerm = GetExampleTerm("2");
        var rbqLeft = new RavenBooleanQuery(operatorType) {{firstTerm, occur}, {secondTerm, occur}};
        rbqLeft.Boost = 2.0f;
        var rbqRight = new RavenBooleanQuery(operatorType) {{GetExampleTerm("2"), occur}, {GetExampleTerm("3"), occur}};


        Assert.False(operatorType is OperatorType.And 
            ? rbqLeft.TryAnd(rbqRight, null) 
            : rbqLeft.TryOr(rbqRight, null));

        Assert.Equal(2, rbqLeft.Clauses.Count);
        Assert.Equal(firstTerm, rbqLeft.Clauses[0].Query);
        Assert.Equal(secondTerm, rbqLeft.Clauses[1].Query);
    }
    
    [Theory]
    [InlineData(OperatorType.And, Occur.MUST)]
    [InlineData(OperatorType.Or, Occur.SHOULD)]
    public void LeftNotBoostedRightBoostedWillBeMerged(OperatorType operatorType, Occur occur)
    {
        var firstTerm = GetExampleTerm("1");
        var secondTerm = GetExampleTerm("2");
        var rbqLeft = new RavenBooleanQuery(operatorType) {{firstTerm, occur}, {secondTerm, occur}};
        var rbqRight = new RavenBooleanQuery(operatorType) {{GetExampleTerm("2"), occur}, {GetExampleTerm("3"), occur}};
        rbqRight.Boost = 2.0f;

        var mergingResult = operatorType is OperatorType.And
            ? rbqLeft.TryAnd(rbqRight, null)
            : rbqLeft.TryOr(rbqRight, null);
        Assert.True(mergingResult);

        Assert.Equal(3, rbqLeft.Clauses.Count);
        Assert.Equal(firstTerm, rbqLeft.Clauses[0].Query);
        Assert.Equal(secondTerm, rbqLeft.Clauses[1].Query);
        Assert.Equal(rbqRight, rbqLeft.Clauses[2].Query); // boxed
    }

    private static Query GetExampleTerm(string term, float? boost = null) => LuceneQueryHelper.Term("exampleField", term, LuceneTermType.String, boost: boost);

    public RavenBooleanQueryTests(ITestOutputHelper output) : base(output)
    {
    }
}
                                                                                                                                                                                                
