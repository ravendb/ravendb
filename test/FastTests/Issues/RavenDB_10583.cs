using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_10583 : RavenTestBase
    {
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

        [Fact]
        public void TranslateEnumAsString()
        {
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = a => a.Conventions.SaveEnumsAsIntegers = false
            }))
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

                    Assert.Equal("declare function output(x) {\r\n\tvar test = x.Value===\"Value1\";\r\n\treturn { ShouldBeTrue : test };\r\n}\r\nfrom Articles as x select output(x)", query.ToString());

                    var result = query.ToList();
                    Assert.Equal(true, result[0].ShouldBeTrue);

                }
            }
        }

        [Fact]
        public void TranslateEnumAsInteger()
        {
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = a => a.Conventions.SaveEnumsAsIntegers = true
            }))
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

                    Assert.Equal("declare function output(x) {\r\n\tvar test = x.Value===0;\r\n\treturn { ShouldBeTrue : test };\r\n}\r\nfrom Articles as x select output(x)", query.ToString());

                    var result = query.ToList();
                    Assert.Equal(true, result[0].ShouldBeTrue);

                }
            }
        }

    }
}
