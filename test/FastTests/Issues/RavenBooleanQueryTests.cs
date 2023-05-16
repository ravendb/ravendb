using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Xunit;
using Query = Lucene.Net.Search.Query;

namespace FastTests.Issues;

//Low-level query optimization tests. 
public class RavenBooleanQueryTests
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

    private static Query GetExampleTerm(string term, float? boost = null) => LuceneQueryHelper.Term("exampleField", term, LuceneTermType.String);
}
