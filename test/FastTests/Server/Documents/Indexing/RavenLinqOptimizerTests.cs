using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class RavenLinqOptimizerTests : RavenTestBase
    {
        [Fact]
        public void CanOptimizeExpression()
        {
            var str = @"
            from u in docs.Users
            from tag in u.Tags
            select new { tag }
            ";

            var result = OptimizeExpression(str);
            Assert.IsType<ForEachStatementSyntax>(result);
            Assert.Equal(result.ToString(),
                "foreach (var u in docs.Users)\r\n{\r\n    foreach (var tag in u.Tags)\r\n    {\r\n        yield return new\r\n        {\r\n        tag\r\n        }\r\n\r\n        ;\r\n    }\r\n}");

            str = @"
            from u in docs.Users
            where u.IsActive
            let address = u.Address
            where address.City != ""Tel Aviv""
            select new { u.Name, u.Email }";

            result = OptimizeExpression(str);
            Assert.IsType<ForEachStatementSyntax>(result);
            Assert.Equal(result.ToString(),
                "foreach (var u in docs.Users)\r\n{\r\n    if ((u.IsActive) == false)\r\n        continue;\r\n    var address = u.Address;\r\n    if ((address.City != \"Tel Aviv\") == false)\r\n        continue;\r\n    yield return new\r\n    {\r\n    u.Name, u.Email\r\n    }\r\n\r\n    ;\r\n}");

            str = @"
                        docs.Books.Select(p => new {
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
                        })";

            result = OptimizeExpression(str);
            Assert.IsType<ForEachStatementSyntax>(result);
            Assert.Equal(result.ToString(),
                "foreach (var p in (docs.Books))\r\n{\r\n    var p0 = new\r\n    {\r\n    Name = p.Name, Category = p.Category, Ratings =\r\n        from x in (p.Ratings)select x.Rate\r\n    }\r\n\r\n    ;\r\n    {\r\n        yield return new\r\n        {\r\n        Category = p0.Category, Books = new object[]{new\r\n        {\r\n        Name = p0.Name, MinRating = DynamicEnumerable.Min(p0.Ratings), MaxRating = DynamicEnumerable.Max(p0.Ratings)}\r\n        }}\r\n\r\n        ;\r\n    }\r\n}");

        }

        private static SyntaxNode OptimizeExpression(string str)
        {
           var expression = SyntaxFactory.ParseExpression(str);
           var result = new RavenLinqPrettifier().Visit(expression).NormalizeWhitespace();
           return new RavenLinqOptimizer().Visit(result).NormalizeWhitespace();
        }

    }
}