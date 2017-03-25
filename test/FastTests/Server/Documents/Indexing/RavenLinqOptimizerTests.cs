using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class RavenLinqOptimizerTests : RavenTestBase
    {
        [Theory]
        [InlineData(@"
            from u in docs.Users
            from tag in u.Tags
            select new { tag }
            ", @"foreach (var u in docs.Users)
{
    foreach (var tag in u.Tags)
    {
        yield return new
        {
        tag
        }

        ;
    }
}")]
        [InlineData(@"
            from u in docs.Users
            where u.IsActive
            let address = u.Address
            where address.City != ""Tel Aviv""
            select new { u.Name, u.Email }", @"foreach (var u in docs.Users)
{
    if (u.IsActive == false)
        continue;
    var address = u.Address;
    if ((address.City != ""Tel Aviv"") == false)
        continue;
    yield return new
    {
    u.Name, u.Email
    }

    ;
}")]
        [InlineData(@"docs.Books.Select(p => new {
                            Name = p.Name,
                            Category = p.Category,
                            Ratings = p.Ratings.Select(x => x.Rate)
                        }).Select(p0 => new {
                            Category = p0.Category,
                            Books = new object[] {
                                new {
                                    Name = p0.Name,
                                    MinRating = DynamicEnumerable.Min(p0.Ratings),
                                    MaxRating = DynamicEnumerable.Max(p0.Ratings)
                                }
                            }
                        })", @"foreach (var p in docs.Books)
{
    var p0 = new
    {
    Name = p.Name, Category = p.Category, Ratings =
        from x in p.Ratings
        select x.Rate
    }

    ;
    {
        yield return new
        {
        Category = p0.Category, Books = new object[]{new
        {
        Name = p0.Name, MinRating = DynamicEnumerable.Min(p0.Ratings), MaxRating = DynamicEnumerable.Max(p0.Ratings)}
        }}

        ;
    }
}")]
        public void CanOptimizeExpression(string code, string optimized)
        {
            var result = OptimizeExpression(code);
            Assert.IsType<ForEachStatementSyntax>(result);

            Assert.Equal(result.ToFullString(), optimized);
        }

        private static SyntaxNode OptimizeExpression(string str)
        {
           var expression = SyntaxFactory.ParseExpression(str.Trim());
           var result = new RavenLinqPrettifier().Visit(expression);
            var expr = new RavenLinqOptimizer().Visit(result);
            expr = expr.ReplaceTrivia(expr.DescendantTrivia(), (t1, t2)  => new SyntaxTrivia());
           return expr.NormalizeWhitespace();
        }

    }
}