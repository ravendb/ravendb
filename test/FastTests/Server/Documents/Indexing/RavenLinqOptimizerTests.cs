using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Server.Documents.Indexes.Static.Roslyn;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Indexing
{
    public class RavenLinqOptimizerTests : NoDisposalNeeded
    {
        public RavenLinqOptimizerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(@"docs.SelectMany(match => match.Teams, (match, team) => new { match = match, team = team })
                          .SelectMany(this0 => this0.team.Series, (this0, serie) => new { this0 = this0, serie = serie })
                          .SelectMany(this1 => this1.serie.Games, (this1, game) => new { Player = game.Player,
                                                                                         Pins = game.Pins,
                                                                                         Series = 1, Score = game.Score,
                                                                                         BestGame = game.Pins,
                                                                                         GamesWithStats = game.Strikes != null ? 1 : 0,
                                                                                         Strikes = game.Strikes,
                                                                                         Misses = game.Misses,
                                                                                         OnePinMisses = game.OnePinMisses,
                                                                                         Splits = game.Splits,
                                                                                         CoveredAll = game.CoveredAll ? 1 : 0 }

)", @"foreach (var match in docs)
{
    foreach (var team in match.Teams)
    {
        var this0 = new
        {
        match = match, team = team
        }

        ;
        foreach (var serie in this0.team.Series)
        {
            var this1 = new
            {
            this0 = this0, serie = serie
            }

            ;
            foreach (var game in this1.serie.Games)
            {
                yield return new
                {
                Player = game.Player, Pins = game.Pins, Series = 1, Score = game.Score, BestGame = game.Pins, GamesWithStats = game.Strikes != null ? 1 : 0, Strikes = game.Strikes, Misses = game.Misses, OnePinMisses = game.OnePinMisses, Splits = game.Splits, CoveredAll = game.CoveredAll ? 1 : 0
                }

                ;
            }
        }
    }
}")]
        [InlineData(@"docs.SelectMany(barn => barn.Households,
                                      (barn, household) => new { barn = barn, household = household })
                          .SelectMany(this0 => this0.household.Members,
                                      (this0, member) => new
                                                         {
                                                              InternalId = this0.barn.InternalId,
                                                              Name = this0.barn.Name,
                                                              HouseholdId = this0.household.InternalId,
                                                              MemberId = member.InternalId,
                                                              MembersName = member.Name
                                                          })"
, @"foreach (var barn in docs)
{
    foreach (var household in barn.Households)
    {
        var this0 = new
        {
        barn = barn, household = household
        }

        ;
        foreach (var member in this0.household.Members)
        {
            yield return new
            {
            InternalId = this0.barn.InternalId, Name = this0.barn.Name, HouseholdId = this0.household.InternalId, MemberId = member.InternalId, MembersName = member.Name
            }

            ;
        }
    }
}")]
        [InlineData(@"docs.SelectMany((Func<dynamic, IEnumerable<dynamic>>)
                                            (user => (IEnumerable<dynamic>)user.Aliases)
                                      ,(Func<dynamic, dynamic, dynamic>)
                                            ((user, alias) => new {user = user, alias = alias}))
                          .Where((Func<dynamic, bool>)(this0 => this0.alias.Length > 3))
                          .Select((Func<dynamic, dynamic>)(this0 => new
                          {
                              Length = this0.alias.Length,
                              Aliases = this0.alias
                          }))"
, @"foreach (var user in docs)
{
    foreach (var alias in user.Aliases)
    {
        var this0 = new
        {
        user = user, alias = alias
        }

        ;
        if ((this0.alias.Length > 3) == false)
            continue;
        yield return new
        {
        Length = this0.alias.Length, Aliases = this0.alias
        }

        ;
    }
}")]
        [InlineData(@"e1.SelectMany(x1 => e2, (x1, x2) => new { x2.v })", @"foreach (var x1 in e1)
{
    foreach (var x2 in e2)
    {
        yield return new
        {
        x2.v
        }

        ;
    }
}")]
        [InlineData(@"docs.Customers.SelectMany(c => c.Orders, (customer, order) => new {Date = order.Date, Product = order.Product})"
, @"foreach (var customer in docs.Customers)
{
    foreach (var order in customer.Orders)
    {
        yield return new
        {
        Date = order.Date, Product = order.Product
        }

        ;
    }
}")]
        [InlineData(@"
            from u2 in (
                from u1 in (
                    from u0 in docs.Users
                    select new { u0.Name }
                )
                select new {u1.Name}
            )
            select new { u2.Name }"
            , @"foreach (var u0 in docs.Users)
{
    var u1 = new
    {
    u0.Name
    }

    ;
    var u2 = new
    {
    u1.Name
    }

    ;
    yield return new
    {
    u2.Name
    }

    ;
}")]
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
        [InlineData(@"from doc in docs.Foos
                                from docBarSomeOtherDictionaryItem in ((IEnumerable<dynamic>)doc.Bar.SomeOtherDictionary).DefaultIfEmpty()
                                from docBarSomeDictionaryItem in ((IEnumerable<dynamic>)doc.Bar.SomeDictionary).DefaultIfEmpty()
                                select new
                                {
                                    Bar_SomeOtherDictionary_Value = docBarSomeOtherDictionaryItem.Value,
                                    Bar_SomeOtherDictionary_Key = docBarSomeOtherDictionaryItem.Key,
                                    Bar_SomeDictionary_Value = docBarSomeDictionaryItem.Value,
                                    Bar_SomeDictionary_Key = docBarSomeDictionaryItem.Key,
                                    Bar = doc.Bar
                                }", @"foreach (var doc in docs.Foos)
{
    foreach (var docBarSomeOtherDictionaryItem in ((IEnumerable<dynamic>)doc.Bar.SomeOtherDictionary).DefaultIfEmpty())
    {
        foreach (var docBarSomeDictionaryItem in ((IEnumerable<dynamic>)doc.Bar.SomeDictionary).DefaultIfEmpty())
        {
            yield return new
            {
            Bar_SomeOtherDictionary_Value = docBarSomeOtherDictionaryItem.Value, Bar_SomeOtherDictionary_Key = docBarSomeOtherDictionaryItem.Key, Bar_SomeDictionary_Value = docBarSomeDictionaryItem.Value, Bar_SomeDictionary_Key = docBarSomeDictionaryItem.Key, Bar = doc.Bar
            }

            ;
        }
    }
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
    Name = p.Name, Category = p.Category, Ratings = p.Ratings.Select(x => x.Rate)}

    ;
    yield return new
    {
    Category = p0.Category, Books = new object[]{new
    {
    Name = p0.Name, MinRating = DynamicEnumerable.Min(p0.Ratings), MaxRating = DynamicEnumerable.Max(p0.Ratings)}
    }}

    ;
}")]
        [InlineData(@"from doc in docs.Foos
let a = doc.Aloha
                                from docBarSomeOtherDictionaryItem in ((IEnumerable<dynamic>)doc.Bar.SomeOtherDictionary).DefaultIfEmpty()
where a != docBarSomeOtherDictionaryItem.Free
                                from docBarSomeDictionaryItem in ((IEnumerable<dynamic>)doc.Bar.SomeDictionary).DefaultIfEmpty()
where docBarSomeDictionaryItem.Item1 != docBarSomeOtherDictionaryItem.Item2
                                select new
                                {
                                    Bar_SomeOtherDictionary_Value = docBarSomeOtherDictionaryItem.Value,
                                    Bar_SomeOtherDictionary_Key = docBarSomeOtherDictionaryItem.Key,
                                    Bar_SomeDictionary_Value = docBarSomeDictionaryItem.Value,
                                    Bar_SomeDictionary_Key = docBarSomeDictionaryItem.Key,
                                    Bar = doc.Bar
                                }", @"foreach (var doc in docs.Foos)
{
    var a = doc.Aloha;
    foreach (var docBarSomeOtherDictionaryItem in ((IEnumerable<dynamic>)doc.Bar.SomeOtherDictionary).DefaultIfEmpty())
    {
        if ((a != docBarSomeOtherDictionaryItem.Free) == false)
            continue;
        foreach (var docBarSomeDictionaryItem in ((IEnumerable<dynamic>)doc.Bar.SomeDictionary).DefaultIfEmpty())
        {
            if ((docBarSomeDictionaryItem.Item1 != docBarSomeOtherDictionaryItem.Item2) == false)
                continue;
            yield return new
            {
            Bar_SomeOtherDictionary_Value = docBarSomeOtherDictionaryItem.Value, Bar_SomeOtherDictionary_Key = docBarSomeOtherDictionaryItem.Key, Bar_SomeDictionary_Value = docBarSomeDictionaryItem.Value, Bar_SomeDictionary_Key = docBarSomeDictionaryItem.Key, Bar = doc.Bar
            }

            ;
        }
    }
}")]
        [InlineData(@"from q in docs.Questions
                    select new
                    {
                        Question = q,
                    } into item
                    select new
                    {
                        Month = item.Question.CreationDate,
                        Users = 1
                    }", @"foreach (var q in docs.Questions)
{
    var item = new
    {
    Question = q, }

    ;
    yield return new
    {
    Month = item.Question.CreationDate, Users = 1
    }

    ;
}")]
        [InlineData(@"docs.Questions.Select(x => new
            {
                Question = x
            }).Select(x => new
            {
                Month = x,
                Users = 1
            })", @"foreach (var x in docs.Questions)
{
    var x = new
    {
    Question = x
    }

    ;
    yield return new
    {
    Month = x, Users = 1
    }

    ;
}")]
        [InlineData(@"from q in docs.Questions
                from a in q.Answers
                select new
                {
                    QuestionCreatedAt = q.CreationDate,
                    AnswerCreatedAt = a.CreationDate,
                    Answers = q.Answers
                }
                into dates
                where dates.QuestionCreatedAt != DateTimeOffset.MinValue
                from answersAgain in dates.Answers
                let a = answersAgain
                select new
                {
                    Answer = a,
                    Dates = dates
                }", @"foreach (var q in docs.Questions)
{
    foreach (var a in q.Answers)
    {
        var dates = new
        {
        QuestionCreatedAt = q.CreationDate, AnswerCreatedAt = a.CreationDate, Answers = q.Answers
        }

        ;
        if ((dates.QuestionCreatedAt != DateTimeOffset.MinValue) == false)
            continue;
        foreach (var answersAgain in dates.Answers)
        {
            var a = answersAgain;
            yield return new
            {
            Answer = a, Dates = dates
            }

            ;
        }
    }
}")]
        [InlineData(@"from q in docs.Questions
                    select new
                    {
                        Id = q.Id,
                        CreatedAt = q.CreatedAt
                    } into first
                    select new
                    {
                        first.CreatedAt
                    } into second
                    select new
                    {
                        CreatedAt = second.CreatedAt,
                    }", @"foreach (var q in docs.Questions)
{
    var first = new
    {
    Id = q.Id, CreatedAt = q.CreatedAt
    }

    ;
    var second = new
    {
    first.CreatedAt
    }

    ;
    yield return new
    {
    CreatedAt = second.CreatedAt, }

    ;
}")]
        public void CanOptimizeExpression(string code, string optimized)
        {
            var result = OptimizeExpression(code);
            Assert.IsType<ForEachStatementSyntax>(result);

            RavenTestHelper.AssertEqualRespectingNewLines(optimized, result.ToFullString());
        }

        private static SyntaxNode OptimizeExpression(string str)
        {
            var expression = SyntaxFactory.ParseExpression(str.Trim());
            var result = new RavenLinqPrettifier().Visit(expression);

            var validator = new FieldNamesValidator();

            validator.Validate(str, SyntaxFactory.ParseExpression(str).NormalizeWhitespace());

            var expr = new RavenLinqOptimizer(validator).Visit(result);

            expr = expr.ReplaceTrivia(expr.DescendantTrivia(), (t1, t2) => new SyntaxTrivia());
            return expr.NormalizeWhitespace();
        }
    }
}
