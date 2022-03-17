using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10244 : RavenTestBase
    {
        public RavenDB_10244(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void MustNotParseStringAsDateTime()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DocumentationPages_ByKey());
                
                using (var session = store.OpenSession())
                {
                    session.Store(new DocumentationPage()
                    {
                        Key = "3.5",
                        Version = "3.5"
                    });
                    
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<DocumentationPages_ByKey.Result, DocumentationPages_ByKey>()
                        .ToList();

                    Assert.Equal(1, list.Count);

                    Assert.Equal("3.5", list[0].Languages.First().Key);
                }
            }
        }
        
        private class DocumentationPages_ByKey : AbstractIndexCreationTask<DocumentationPage, DocumentationPages_ByKey.Result>
        {
            public class Result
            {
                public string Key { get; set; }

                public Dictionary<string, string> Ids { get; set; }

                public Dictionary<string, string[]> Languages { get; set; }
            }

            public DocumentationPages_ByKey()
            {
                Map =
                    pages =>
                        from page in pages
                        select new
                        {
                            Key = page.Key,
                            Ids = new Dictionary<string, string>
                            {
                                { page.Version + "/" + page.Language, page.Id }
                            },
                            Languages = new Dictionary<string, string[]>
                            {
                                { page.Version, new [] { page.Language.ToString() } }
                            }
                        };

                Reduce = results => from result in results
                    group result by result.Key
                    into g
                    select new
                    {
                        Key = g.Key,
                        Ids = g.SelectMany(x => x.Ids).ToDictionary(x => x.Key, x => x.Value),
                        Languages = g.SelectMany(x => x.Languages).GroupBy(x => x.Key).ToDictionary(x => x.Key, x => x.SelectMany(y => y.Value).Distinct())
                    };
            }
        }
        
        private class DocumentationPage
        {
            public string Version { get; set; }

            public string Key { get; set; }

            public string Id { get; set; }

            public Language Language { get; set; }

        }
        
        private enum Language
        {
            [Description("C#")]
            Csharp,

            [Description("Java")]
            Java,

            [Description("HTTP")]
            Http,

            [Description("Python")]
            Python,

            [Description("General")]
            All
        }
    }
}
