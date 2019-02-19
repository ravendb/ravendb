using System.Collections.Generic;
using System.Linq;
using FastTests;
using Lucene.Net.Analysis;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class WiseShrek : RavenTestBase
    {
        private class Soft
        {
            public int f_platform { get; set; }
            public string f_name { get; set; }
            public string f_alias { get; set; }
            public string f_License { get; set; }
            public int f_totaldownload { get; set; }
        }

        [Fact(Skip = "Missing features ")]
        public void Isolated()
        {
            //var ramDirectory = new RAMDirectory();
            //using (new IndexWriter(ramDirectory, new StandardAnalyzer(Version.LUCENE_29), IndexWriter.MaxFieldLength.UNLIMITED)) { }
            //var inMemoryRavenConfiguration = new InMemoryRavenConfiguration();
            //inMemoryRavenConfiguration.Initialize();

            //var fieldOptions1 = new IndexFieldOptions { Indexing = FieldIndexing.NotAnalyzed };
            //var fieldOptions2 = new IndexFieldOptions { Indexing = FieldIndexing.NotAnalyzed, Sort = SortOptions.Numeric };
            //var fieldOptions3 = new IndexFieldOptions { Indexing = FieldIndexing.Analyzed, Analyzer = typeof(KeywordAnalyzer).AssemblyQualifiedName };

            //var simpleIndex = new SimpleIndex(ramDirectory, 0, new IndexDefinition
            //{

            //    Maps = { @"from s in docs.Softs select new { s.f_platform, s.f_name, s.f_alias,s.f_License,s.f_totaldownload}" },

            //    Fields =
            //        {
            //            { "f_platform" , fieldOptions1 },
            //            {"f_License" , fieldOptions2 },
            //            {"f_totaldownload" , fieldOptions2 },
            //            {"f_name" , fieldOptions3 },
            //            {"f_alias" , fieldOptions3 }
            //        }

            //}, new MapOnlyView(), new WorkContext()
            //{
            //    Configuration = inMemoryRavenConfiguration
            //});

            //var perFieldAnalyzerWrapper = RavenPerFieldAnalyzerWrapper.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), new List<Action>());

            //var tokenStream = perFieldAnalyzerWrapper.TokenStream("f_name", new StringReader("hello Shrek"));
            //while (tokenStream.IncrementToken())
            //{
            //    var attribute = (TermAttribute)tokenStream.GetAttribute<ITermAttribute>();
            //    Assert.Equal("hello Shrek", attribute.Term);
            //}
        }

        [Fact]
        public void UsingKeywordAnalyzing()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                var fieldOptions1 = new IndexFieldOptions { Indexing = FieldIndexing.Exact };
                var fieldOptions2 = new IndexFieldOptions { Indexing = FieldIndexing.Exact, };
                var fieldOptions3 = new IndexFieldOptions { Indexing = FieldIndexing.Search, Analyzer = typeof(KeywordAnalyzer).AssemblyQualifiedName };

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { @"from s in docs.Softs select new { s.f_platform, s.f_name, s.f_alias,s.f_License,s.f_totaldownload}" },

                    Fields =
                    {
                        { "f_platform" , fieldOptions1 },
                        {"f_License" , fieldOptions2 },
                        {"f_totaldownload" , fieldOptions2 },
                        {"f_name" , fieldOptions3 },
                        {"f_alias" , fieldOptions3 }
                    },
                    Name = "test"

                }}));

                Soft entity = new Soft
                {
                    f_platform = 1,
                    f_name = "hello Shrek",
                    f_alias = "world",
                    f_License = "agpl",
                    f_totaldownload = -1
                };
                session.Store(entity);
                session.Advanced.GetMetadataFor(entity)["@collection"] = "Softs";
                session.SaveChanges();

                List<Soft> tmps = session.Advanced.DocumentQuery<Soft>("test")
                    .WaitForNonStaleResults()
                    .WhereStartsWith("f_name", "s")
                    .OrderByDescending("f_License")
                    .OrderBy("f_totaldownload")
                    .ToList();

                Assert.Empty(tmps);
            }
        }
    }
}
