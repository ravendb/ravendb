using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class linmouhong3 : RavenTestBase
    {
        private class ShortUrlMap
        {
            public string LongUrl { get; set; }

            public string ShortUrl { get; set; }
        }

        private class ShortUrlMapIndex : AbstractIndexCreationTask<ShortUrlMap>
        {
            public ShortUrlMapIndex()
            {
                Map = maps => from m in maps
                              select new
                              {
                                  m.ShortUrl,
                                  m.LongUrl
                              };
            }
        }

        [Fact]
        public void InQueriesWork()
        {
            using (var store = GetDocumentStore())
            {
                new ShortUrlMapIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ShortUrlMap
                    {
                        LongUrl = "http://www.a.com",
                        ShortUrl = "http://t.cn/abc"
                    });
                    session.Store(new ShortUrlMap
                    {
                        LongUrl = "http://www.abcdef-134234.com",
                        ShortUrl = "http://t.cn/def"
                    });
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var longUrls = new List<string>
                    {
                        "http://www.a.com",
                        "http://ctow.sigcms.com/click?"
                    };

                    var query1 = session.Query<ShortUrlMap, ShortUrlMapIndex>()
                                        .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                                        .Where(x => x.LongUrl.In(longUrls))
                                        .ToList();
                    Assert.Equal(1, query1.Count);

                }

            }
        }

    }
}
