using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries
{
    public class RegexQueryTests : RavenTestBase
    {
        public RegexQueryTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void QueriesWithRegexShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RegexMe { Text = "I love dogs and cats" });
                    session.Store(new RegexMe { Text = "I love cats" });
                    session.Store(new RegexMe { Text = "I love dogs" });
                    session.Store(new RegexMe { Text = "I love bats" });
                    session.Store(new RegexMe { Text = "dogs love me" });
                    session.Store(new RegexMe { Text = "cats love me" });
                    session.SaveChanges();
                    var res = session.Advanced.RawQuery<RegexMe>("from RegexMes as r where Regex(r.Text,\"^[a-z ]{2,4}love\")")
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(3))
                        .ToList();
                    Assert.Equal(4, res.Count);
                }
            }
        }

        [Fact]
        public async Task QueriesWithRegexFromDocumentQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RegexMe { Text = "I love dogs and cats" });
                    session.Store(new RegexMe { Text = "I love cats" });
                    session.Store(new RegexMe { Text = "I love dogs" });
                    session.Store(new RegexMe { Text = "I love bats" });
                    session.Store(new RegexMe { Text = "dogs love me" });
                    session.Store(new RegexMe { Text = "cats love me" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<RegexMe>()
                        .WhereRegex(x => x.Text, "^[a-z ]{2,4}love");

                    var iq = query.GetIndexQuery();

                    Assert.Equal("from 'RegexMes' where regex(Text, $p0)", iq.Query);
                    Assert.Equal("^[a-z ]{2,4}love", iq.QueryParameters["p0"]);

                    var result = query.ToList();
                    Assert.Equal(4, result.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced.AsyncDocumentQuery<RegexMe>()
                        .WhereRegex(x => x.Text, "^[a-z ]{2,4}love");

                    var iq = query.GetIndexQuery();

                    Assert.Equal("from 'RegexMes' where regex(Text, $p0)", iq.Query);
                    Assert.Equal("^[a-z ]{2,4}love", iq.QueryParameters["p0"]);

                    var result = await query.ToListAsync();
                    Assert.Equal(4, result.Count);
                }
            }
        }

        [Fact]
        public void QueriesWithRegexFromLinqProvider()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RegexMe { Text = "I love dogs and cats" });
                    session.Store(new RegexMe { Text = "I love cats" });
                    session.Store(new RegexMe { Text = "I love dogs" });
                    session.Store(new RegexMe { Text = "I love bats" });
                    session.Store(new RegexMe { Text = "dogs love me" });
                    session.Store(new RegexMe { Text = "cats love me" });
                    session.SaveChanges();

                    var query = session.Query<RegexMe>().Where(x => Regex.IsMatch(x.Text, "^[a-z ]{2,4}love"));

                    Assert.Equal("from 'RegexMes' where regex(Text, $p0)", query.ToString());

                    var result = query.ToList();
                    Assert.Equal(4, result.Count);
                }
            }
        }

        [Fact]
        public void QueriesWithRegexFromLinqProvider_QueryExpressionSyntax()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RegexMe { Text = "I love dogs and cats" });
                    session.Store(new RegexMe { Text = "I love cats" });
                    session.Store(new RegexMe { Text = "I love dogs" });
                    session.Store(new RegexMe { Text = "I love bats" });
                    session.Store(new RegexMe { Text = "dogs love me" });
                    session.Store(new RegexMe { Text = "cats love me" });
                    session.SaveChanges();

                    var query = (from r in session.Query<RegexMe>()
                                 where Regex.IsMatch(r.Text, "^[a-z ]{2,4}love")
                                 select r.Text
                                 ).ToList();

                    Assert.Equal(4, query.Count);
                }
            }
        }

        [Fact]
        public void QueriesWithRegexAndEscapedCharsShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RegexMe { Text = "I love dogs and cats" });
                    session.Store(new RegexMe { Text = "I love cats" });
                    session.Store(new RegexMe { Text = "I love dogs" });
                    session.Store(new RegexMe { Text = "I love bats" });
                    session.Store(new RegexMe { Text = "dogs love me" });
                    session.Store(new RegexMe { Text = "cats love me" });
                    session.SaveChanges();
                    var res = session.Advanced.RawQuery<RegexMe>("from RegexMes as r where Regex(r.Text,\"^(\\\\w+\\\\s+){4}\\\\w+$\")")
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(3))
                        .ToList();
                    Assert.Equal(1, res.Count);
                }
            }
        }


        public class RegexMe
        {
            public string Text { get; set; }
        }
    }
}
