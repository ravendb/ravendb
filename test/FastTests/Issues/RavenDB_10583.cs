using Tests.Infrastructure;
using System;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_10583 : RavenTestBase
    {
        public RavenDB_10583(ITestOutputHelper output) : base(output)
        {
        }

        private class Article
        {
            public ArticleEnum Value { get; set; }
        }
        private enum ArticleEnum
        {
            Value1,
            Value2,
            Value3
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void TranslateEnumAsString(Options options)
        {
            options.ModifyDocumentStore = a => a.Conventions.SaveEnumsAsIntegers = false;

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                    {
                        Value = ArticleEnum.Value1
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from x in session.Query<Article>()
                                let test = x.Value == ArticleEnum.Value1
                                select new
                                {
                                    ShouldBeTrue = test
                                };

                    var expectedQuery =
                        $"declare function output(x) {{{Environment.NewLine}\tvar test = x?.Value==='Value1';{Environment.NewLine}\treturn {{ ShouldBeTrue : test }};{Environment.NewLine}}}{Environment.NewLine}from 'Articles' as x select output(x)";

                    Assert.Equal(expectedQuery, query.ToString());

                    var result = query.ToList();
                    Assert.Equal(true, result[0].ShouldBeTrue);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void TranslateEnumAsInteger(Options options)
        {
            options.ModifyDocumentStore = a => a.Conventions.SaveEnumsAsIntegers = true;
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                    {
                        Value = ArticleEnum.Value1
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from x in session.Query<Article>()
                                let test = x.Value == ArticleEnum.Value1
                                select new
                                {
                                    ShouldBeTrue = test
                                };

                    var expectedQuery =
                        $"declare function output(x) {{{Environment.NewLine}\tvar test = x?.Value===0;{Environment.NewLine}\treturn {{ ShouldBeTrue : test }};{Environment.NewLine}}}{Environment.NewLine}from 'Articles' as x select output(x)";

                    Assert.Equal(expectedQuery, query.ToString());

                    var result = query.ToList();
                    Assert.Equal(true, result[0].ShouldBeTrue);
                }
            }
        }

    }
}
