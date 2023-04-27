using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17130 : RavenTestBase
    {
        public RavenDB_17130(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void GetDocumentsFromIndexWithIfEntityIs_Lucene(Options options) => GetDocumentsFromIndexWithIfEntityIs<Color_ForSearch>(options);
        
        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
        public void GetDocumentsFromIndexWithIfEntityIs_Corax(Options options) => GetDocumentsFromIndexWithIfEntityIs<Color_ForSearch_Corax>(options);
        
        private void GetDocumentsFromIndexWithIfEntityIs<TIndex>(Options options) where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                store.ExecuteIndex(new TIndex());
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Color
                        {
                            Name = $"Color name {i}",
                            CreatedDate = DateTime.UtcNow.AddDays(i + 1)
                        });
                    }

                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }
                
                Indexes.WaitForIndexing(store);

                var countOfDocuments = 0L;
                store.Maintenance.ForTesting(() => new GetStatisticsOperation()).AssertAll((key, statistics) =>  countOfDocuments += statistics.CountOfDocuments);
                Assert.Equal(13, countOfDocuments); // 10 colors + 1 company + 2x hilo

                using (var session = store.OpenSession())
                {
                    var documentQuery = session.Advanced.DocumentQuery<Color_ForSearch_Corax.Result, TIndex>().ToList();
                    Assert.Equal(10, documentQuery.Count);
                }
            }
        }

        private class Color_ForSearch : AbstractIndexCreationTask<object, Color_ForSearch_Corax.Result>
        {
            public Color_ForSearch()
            {
                Map = colors => from colorDoc in colors
                                let color = colorDoc.IfEntityIs<Color>("Colors")
                                where color != null
                                select new
                                {
                                    color.Name,
                                    color.CreatedDate,
                                    Query = AsJson(color).Select(x => x.Value),
                                };
            }
        }
        
        private class Color_ForSearch_Corax : AbstractIndexCreationTask<object, Color_ForSearch_Corax.Result>
        {
            public class Result : Color
            {
                public string Query { get; set; }
            }

            public Color_ForSearch_Corax()
            {
                Map = colors => from colorDoc in colors
                    let color = colorDoc.IfEntityIs<Color>("Colors")
                    where color != null
                    select new
                    {
                        color.Name,
                        color.CreatedDate,
                        Query = AsJson(color).Where(x => x.Key != "@metadata").Select(x => x.Value),
                    };
            }
        }

        private class Color
        {
            public string Name { get; set; }
            public DateTime CreatedDate { get; set; }
        }
    }
}
