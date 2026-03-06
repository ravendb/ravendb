using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25508(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public void CanSortByScoreWhenMultiVectorSearchIsSkippedInBinaryMatch()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenSession();
        session.Store(new Dto("Yes", "Yes"));
        session.SaveChanges();

        var result = session.Advanced.DocumentQuery<Dto>()
            .OpenSubclause()
                .WhereIn(x => x.Name, ["Yes"])
                    .AndAlso()
                .VectorSearch(f => f.WithText(p => p.Vector), v => v.ByTexts(["Yes", "Something"]))
            .CloseSubclause()
            .OrElse()
            .OpenSubclause()
                .WhereLessThan(x => x.Name, "Yes")
                    .AndAlso()
                .VectorSearch(f => f.WithText(p => p.Vector), v => v.ByTexts(["Yes", "Something"]))
            .CloseSubclause()
            .OrderByScore()
            .ToList();
        Assert.NotEmpty(result);
    }
    
    [RavenMultiplatformFact(RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public void CanSortByScoreWhenVectorSearchIsSkippedInBinaryMatch()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenSession();
        session.Store(new Dto("Yes", "Yes"));
        session.SaveChanges();

        var result = session.Advanced.DocumentQuery<Dto>()
            .OpenSubclause()
                .WhereIn(x => x.Name, ["Yes"])
                    .AndAlso()
                .VectorSearch(f => f.WithText(p => p.Vector), v => v.ByText("Yes"))
            .CloseSubclause()
            .OrElse()
            .OpenSubclause()
                .WhereLessThan(x => x.Name, "Yes")
                    .AndAlso()
                    .VectorSearch(f => f.WithText(p => p.Vector), v => v.ByText("Yes"))
            .CloseSubclause()
            .OrderByScore()
            .ToList();
        Assert.NotEmpty(result);
    }

    private record Dto(string Name, string Vector);
}
