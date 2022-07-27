using System.Collections.Generic;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14361 : RavenTestBase
    {
        public RavenDB_14361(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public void CanProjectFromDictionaryByKeyWhereKeyHasDot_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Test
                    {
                        Name = "aviv",
                        Headers = new Dictionary<string, string>
                        {
                            {"ABC.DEF","205fb229-2373-49da-9329-ab0c01096c6c" },
                            {"ABC.PQR","605fb229-2373-49da-9329-ab0c01096c6c" }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Advanced.RawQuery<dynamic>("from Tests select Headers.'ABC.DEF'");
                    var f = q.FirstOrDefault();
                    Assert.NotNull(f);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanProjectFromDictionaryByKeyWhereKeyHasDot_SessionQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Test
                    {
                        Name = "aviv",
                        Headers = new Dictionary<string, string>
                        {
                            {"ABC.DEF","205fb229-2373-49da-9329-ab0c01096c6c" },
                            {"ABC.PQR","605fb229-2373-49da-9329-ab0c01096c6c" }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Test>()
                        .Select(x => x.Headers["'ABC.DEF'"]);

                    var str = q.FirstOrDefault();
                    Assert.Equal("205fb229-2373-49da-9329-ab0c01096c6c", str);
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Test>()
                        .Select(x => new
                        {
                            Value  = x.Headers["ABC.DEF"]
                        });

                    var f = q.FirstOrDefault();
                    Assert.Equal("205fb229-2373-49da-9329-ab0c01096c6c", f?.Value);
                }
            }
        }

        private class Test
        {
            public string Name { get; set; }

            public Dictionary<string, string> Headers { get; set; }
        }
    }
}
