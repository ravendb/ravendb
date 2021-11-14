using System;
using System.Linq;
using FastTests.Server.JavaScript;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_10644 : RavenTestBase
    {
        public RavenDB_10644(ITestOutputHelper output) : base(output)
        {
        }

        private class Article
        {
            public decimal Value { get; set; }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void TranslateMathRound(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                    {
                        Value = 2.5555555M
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from x in session.Query<Article>()
                                select new
                                {
                                    Round = Math.Round(x.Value),
                                    Round2 = Math.Round(x.Value, 2),
                                    Round4 = Math.Round(x.Value, 4)
                                };

                    Assert.Equal("from 'Articles' as x select { Round : Math.round(x?.Value), Round2 : Math.round(x?.Value * Math.pow(10, 2)) / Math.pow(10, 2), Round4 : Math.round(x?.Value * Math.pow(10, 4)) / Math.pow(10, 4) }", query.ToString());

                    var result = query.ToList();
                    Assert.Equal(3, result[0].Round);
                    Assert.Equal(2.56M, result[0].Round2);
                    Assert.Equal(2.5556M, result[0].Round4);

                }
            }
        }

    }
}
