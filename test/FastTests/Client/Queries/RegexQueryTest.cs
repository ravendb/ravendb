using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace FastTests.Client.Queries
{
    public class RegexQueryTest : RavenTestBase
    {
        [Fact]
        public void QueriesWithRegexShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RegexMe{Text = "I love dogs and cats"});
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
