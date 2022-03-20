using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.MoreLikeThis
{
    public class MoreLikeThisTests : RavenTestBase
    {
        public MoreLikeThisTests(ITestOutputHelper output) : base(output)
        {
        }


        private static string GetLorem(int numWords)
        {
            const string theLorem = "Morbi nec purus eu libero interdum laoreet Nam metus quam posuere in elementum eget egestas eget justo Aenean orci ligula ullamcorper nec convallis non placerat nec lectus Quisque convallis porta suscipit Aliquam sollicitudin ligula sit amet libero cursus egestas Maecenas nec mauris neque at faucibus justo Fusce ut orci neque Nunc sodales pulvinar lobortis Praesent dui tellus fermentum sed faucibus nec faucibus non nibh Vestibulum adipiscing porta purus ut varius mi pulvinar eu Nam sagittis sodales hendrerit Vestibulum et tincidunt urna Fusce lacinia nisl at luctus lobortis lacus quam rhoncus risus a posuere nulla lorem at nisi Sed non erat nisl Cras in augue velit a mattis ante Etiam lorem dui elementum eget facilisis vitae viverra sit amet tortor Suspendisse potenti Nunc egestas accumsan justo viverra viverra Sed faucibus ullamcorper mauris ut pharetra ligula ornare eget Donec suscipit luctus rhoncus Pellentesque eget justo ac nunc tempus consequat Nullam fringilla egestas leo Praesent condimentum laoreet magna vitae luctus sem cursus sed Mauris massa purus suscipit ac malesuada a accumsan non neque Proin et libero vitae quam ultricies rhoncus Praesent urna neque molestie et suscipit vestibulum iaculis ac nulla Integer porta nulla vel leo ullamcorper eu rhoncus dui semper Donec dictum dui";
            var loremArray = theLorem.Split();
            var output = new StringBuilder();
            var rnd = new Random();

            for (var i = 0; i < numWords; i++)
            {
                output.Append(loremArray[rnd.Next(0, loremArray.Length - 1)]).Append(" ");
            }
            return output.ToString();
        }

        private static List<Data> GetDataList()
        {
            var list = new List<Data>
                {
                    new Data {Body = "This is a test. Isn't it great? I hope I pass my test!"},
                    new Data {Body = "I have a test tomorrow. I hate having a test"},
                    new Data {Body = "Cake is great."},
                    new Data {Body = "This document has the word test only once"},
                    new Data {Body = "test"},
                    new Data {Body = "test"},
                    new Data {Body = "test"},
                    new Data {Body = "test"}
                };

            return list;
        }

        [Fact]
        public void CanGetResultsUsingTermVectors()
        {
            using (var store = GetDocumentStore())
            {
                string id;

                using (var session = store.OpenSession())
                {
                    new DataIndex(true, false).Execute(store);

                    var list = GetDataList();
                    list.ForEach(session.Store);
                    session.SaveChanges();

                    id = session.Advanced.GetDocumentId(list.First());
                    Indexes.WaitForIndexing(store);
                }

                AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(store, id);
            }
        }

        [Fact]
        public void CanGetResultsUsingTermVectorsWithDocumentQuery()
        {
            using (var store = GetDocumentStore())
            {
                string id;

                using (var session = store.OpenSession())
                {
                    new DataIndex(true, false).Execute(store);

                    var list = GetDataList();
                    list.ForEach(session.Store);
                    session.SaveChanges();

                    id = session.Advanced.GetDocumentId(list.First());
                    Indexes.WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Advanced.DocumentQuery<Data, DataIndex>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.WhereEquals(y => y.Id, id)).WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Body" }
                        }))
                        .ToList();

                    Assert.NotEmpty(list);
                }
            }
        }

        [Fact]
        public async Task CanGetResultsUsingTermVectorsAsync()
        {
            using (var store = GetDocumentStore())
            {
                string id;

                using (var session = store.OpenSession())
                {
                    new DataIndex(true, false).Execute(store);

                    var list = GetDataList();
                    list.ForEach(session.Store);
                    session.SaveChanges();

                    id = session.Advanced.GetDocumentId(list.First());
                    Indexes.WaitForIndexing(store);
                }

                await AssetMoreLikeThisHasMatchesForAsync<Data, DataIndex>(store, id);
            }
        }

        [Fact]
        public void CanGetResultsUsingStorage()
        {
            using (var store = GetDocumentStore())
            {
                string id;

                using (var session = store.OpenSession())
                {
                    new DataIndex(false, true).Execute(store);

                    var list = GetDataList();
                    list.ForEach(session.Store);
                    session.SaveChanges();

                    id = session.Advanced.GetDocumentId(list.First());
                    Indexes.WaitForIndexing(store);
                }

                AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(store, id);
            }
        }

        [Fact]
        public void CanGetResultsUsingTermVectorsAndStorage()
        {
            using (var store = GetDocumentStore())
            {
                string id;

                using (var session = store.OpenSession())
                {
                    new DataIndex(true, true).Execute(store);

                    var list = GetDataList();
                    list.ForEach(session.Store);
                    session.SaveChanges();

                    id = session.Advanced.GetDocumentId(list.First());
                    Indexes.WaitForIndexing(store);
                }

                AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(store, id);
            }
        }

        [Fact]
        public void CanCompareDocumentsWithIntegerIdentifiers()
        {
            using (var store = GetDocumentStore())
            {
                string id;

                using (var session = store.OpenSession())
                {
                    new OtherDataIndex().Execute(store);

                    var dataQueriedFor = new DataWithIntegerId { Id = "123", Body = "This is a test. Isn't it great? I hope I pass my test!" };

                    var list = new List<DataWithIntegerId>
                    {
                        dataQueriedFor,
                        new DataWithIntegerId {Id = "234", Body = "I have a test tomorrow. I hate having a test"},
                        new DataWithIntegerId {Id = "3456", Body = "Cake is great."},
                        new DataWithIntegerId {Id = "3457", Body = "This document has the word test only once"},
                        new DataWithIntegerId {Id = "3458", Body = "test"},
                        new DataWithIntegerId {Id = "3459", Body = "test"},
                    };
                    list.ForEach(session.Store);
                    session.SaveChanges();

                    id = session.Advanced.GetDocumentId(dataQueriedFor);

                    Indexes.WaitForIndexing(store);
                }

                AssetMoreLikeThisHasMatchesFor<DataWithIntegerId, OtherDataIndex>(store, id);

                id = id.ToLower();
                AssetMoreLikeThisHasMatchesFor<DataWithIntegerId, OtherDataIndex>(store, id);
            }
        }

        [Fact]
        public void CanGetResultsWhenIndexHasSlashInIt()
        {
            using (var store = GetDocumentStore())
            {
                const string key = "datas/1-A";

                using (var session = store.OpenSession())
                {
                    new DataIndex().Execute(store);

                    var list = GetDataList();
                    list.ForEach(session.Store);
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);
                }

                AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(store, key);
            }
        }

        [Fact]
        public void Query_On_Document_That_Does_Not_Have_High_Enough_Word_Frequency()
        {
            using (var store = GetDocumentStore())
            {
                const string key = "datas/4-A";

                using (var session = store.OpenSession())
                {
                    new DataIndex().Execute(store);

                    var list = GetDataList();
                    list.ForEach(session.Store);
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var indexName = new DataIndex().IndexName;

                    var list = session.Query<Data>(indexName)
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == key).WithOptions(new MoreLikeThisOptions
                        {
                            MinimumDocumentFrequency = 5,
                            MinimumTermFrequency = 2,
                            Fields = new[] { "Body" }
                        }))
                        .ToList();

                    Indexes.WaitForIndexing(store);

                    Assert.Empty(list);
                }
            }
        }

        [Fact]
        public void Test_With_Lots_Of_Random_Data()
        {
            using (var store = GetDocumentStore())
            {
                var key = "datas/1-A";
                using (var session = store.OpenSession())
                {
                    new DataIndex().Execute(store);

                    for (var i = 0; i < 100; i++)
                    {
                        var data = new Data { Body = GetLorem(200) };
                        session.Store(data);
                    }
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);
                }

                AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(store, key);
            }
        }

        [Fact]
        public void Do_Not_Pass_FieldNames()
        {
            using (var store = GetDocumentStore())
            {
                var key = "datas/1-A";
                using (var session = store.OpenSession())
                {
                    new DataIndex().Execute(store);

                    for (var i = 0; i < 10; i++)
                    {
                        var data = new Data { Body = "Body" + i, WhitespaceAnalyzerField = "test test" };
                        session.Store(data);
                    }
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Data, DataIndex>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == key))
                        .ToList();

                    Assert.NotEmpty(list);
                }
            }
        }

        [Fact]
        public void Each_Field_Should_Use_Correct_Analyzer()
        {
            using (var store = GetDocumentStore())
            {
                var key = "datas/1-A";
                using (var session = store.OpenSession())
                {
                    new DataIndex().Execute(store);

                    for (var i = 0; i < 10; i++)
                    {
                        var data = new Data { WhitespaceAnalyzerField = "bob@hotmail.com hotmail" };
                        session.Store(data);
                    }
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Data, DataIndex>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == key).WithOptions(new MoreLikeThisOptions
                        {
                            MinimumTermFrequency = 2,
                            MinimumDocumentFrequency = 5
                        }))
                        .ToList();

                    Assert.Empty(list);
                }

                key = "datas/11-A";
                using (var session = store.OpenSession())
                {
                    new DataIndex().Execute(store);

                    for (var i = 0; i < 10; i++)
                    {
                        var data = new Data { WhitespaceAnalyzerField = "bob@hotmail.com bob@hotmail.com" };
                        session.Store(data);
                    }
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Data, DataIndex>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == key))
                        .ToList();

                    Assert.NotEmpty(list);
                }
            }
        }

        [Fact]
        public void Can_Use_Min_Doc_Freq_Param()
        {
            using (var store = GetDocumentStore())
            {
                const string key = "datas/1-A";

                using (var session = store.OpenSession())
                {
                    new DataIndex().Execute(store);

                    var list = new List<Data>
                    {
                        new Data {Body = "This is a test. Isn't it great? I hope I pass my test!"},
                        new Data {Body = "I have a test tomorrow. I hate having a test"},
                        new Data {Body = "Cake is great."},
                        new Data {Body = "This document has the word test only once"}
                    };
                    list.ForEach(session.Store);

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Data, DataIndex>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == key).WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Body" },
                            MinimumDocumentFrequency = 2
                        }))
                        .ToList();

                    Assert.NotEmpty(list);
                }
            }
        }

        [Fact]
        public void Can_Use_Boost_Param()
        {
            using (var store = GetDocumentStore())
            {
                const string key = "datas/1-A";

                using (var session = store.OpenSession())
                {
                    new DataIndex().Execute(store);

                    var list = new List<Data>
                    {
                        new Data {Body = "This is a test. it is a great test. I hope I pass my great test!"},
                        new Data {Body = "Cake is great."},
                        new Data {Body = "I have a test tomorrow."}
                    };
                    list.ForEach(session.Store);

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Data, DataIndex>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == key).WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Body" },
                            MinimumWordLength = 3,
                            MinimumDocumentFrequency = 1,
                            MinimumTermFrequency=2,
                            Boost = true
                        }))
                        .ToList();

                    Assert.NotEqual(0, list.Count);
                    Assert.Equal("I have a test tomorrow.", list[0].Body);
                }
            }
        }

        [Fact]
        public void Can_Use_Stop_Words()
        {
            using (var store = GetDocumentStore())
            {
                const string key = "datas/1-A";

                using (var session = store.OpenSession())
                {
                    new DataIndex().Execute(store);

                    var list = new List<Data>
                    {
                        new Data {Body = "This is a test. Isn't it great? I hope I pass my test!"},
                        new Data {Body = "I should not hit this document. I hope"},
                        new Data {Body = "Cake is great."},
                        new Data {Body = "This document has the word test only once"},
                        new Data {Body = "test"},
                        new Data {Body = "test"},
                        new Data {Body = "test"},
                        new Data {Body = "test"}
                    };
                    list.ForEach(session.Store);

                    session.Store(new MoreLikeThisStopWords { Id = "Config/Stopwords", StopWords = new List<string> { "I", "A", "Be" } });

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var indexName = new DataIndex().IndexName;

                    var list = session.Query<Data, DataIndex>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == key).WithOptions(new MoreLikeThisOptions
                        {
                            StopWordsDocumentId = "Config/Stopwords",
                            MinimumDocumentFrequency = 1,
                            MinimumTermFrequency = 2
                        }))
                        .ToList();

                    Assert.Equal(5, list.Count());
                }
            }
        }

        [Fact]
        public void CanMakeDynamicDocumentQueries()
        {
            using (var store = GetDocumentStore())
            {
                new DataIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var list = GetDataList();
                    list.ForEach(session.Store);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Data, DataIndex>()
                        .MoreLikeThis(f => f.UsingDocument("{ \"Body\": \"A test\" }").WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Body" },
                            MinimumTermFrequency = 1,
                            MinimumDocumentFrequency = 1
                        }))
                        .ToList();

                    Assert.Equal(7, list.Count());
                }
            }
        }

        [Fact]
        public void CanMakeDynamicDocumentQueriesWithComplexProperties()
        {
            using (var store = GetDocumentStore())
            {
                new ComplexDataIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ComplexData
                    {
                        Property = new ComplexProperty
                        {
                            Body = "test"
                        }
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<ComplexData, ComplexDataIndex>()
                        .MoreLikeThis(f => f.UsingDocument("{ \"Property\": { \"Body\": \"test\" } }").WithOptions(new MoreLikeThisOptions
                        {
                            MinimumTermFrequency = 1,
                            MinimumDocumentFrequency = 1
                        }))
                        .ToList();

                    Assert.Equal(1, list.Count);
                }
            }
        }

        private static void AssetMoreLikeThisHasMatchesFor<T, TIndex>(IDocumentStore store, string documentKey)
            where TIndex : AbstractIndexCreationTask, new()
            where T : Identity
        {
            using (var session = store.OpenSession())
            {
                var list = session.Query<T, TIndex>()
                    .MoreLikeThis(f => f.UsingDocument(x => x.Id == documentKey).WithOptions(new MoreLikeThisOptions
                    {
                        Fields = new[] { "Body" }
                    }))
                    .ToList();

                Assert.NotEmpty(list);
            }
        }

        private static async Task AssetMoreLikeThisHasMatchesForAsync<T, TIndex>(IDocumentStore store, string documentKey)
            where TIndex : AbstractIndexCreationTask, new()
            where T : Identity
        {
            using (var session = store.OpenAsyncSession())
            {
                var list = await session.Query<T, TIndex>()
                    .MoreLikeThis(f => f.UsingDocument(x => x.Id == documentKey).WithOptions(new MoreLikeThisOptions
                    {
                        Fields = new[] { "Body" }
                    }))
                    .ToListAsync();

                Assert.NotEmpty(list);
            }
        }

        private abstract class Identity
        {
            public string Id { get; set; }
        }

        private class Data : Identity
        {
            public string Body { get; set; }
            public string WhitespaceAnalyzerField { get; set; }
            public string PersonId { get; set; }
        }

        private class DataWithIntegerId : Identity
        {
            public string Body { get; set; }
        }

        private class ComplexData
        {
            public string Id { get; set; }
            public ComplexProperty Property { get; set; }
        }

        private class ComplexProperty
        {
            public string Body { get; set; }
        }

        private class DataIndex : AbstractIndexCreationTask<Data>
        {
            public DataIndex() : this(true, false)
            {

            }

            public DataIndex(bool termVector, bool store)
            {
                Map = docs => from doc in docs
                              select new { doc.Body, doc.WhitespaceAnalyzerField };

                Analyzers = new Dictionary<Expression<Func<Data, object>>, string>
                {
                    {
                        x => x.Body,
                        typeof (StandardAnalyzer).FullName
                    },
                    {
                        x => x.WhitespaceAnalyzerField,
                        typeof (WhitespaceAnalyzer).FullName
                    }
                };


                if (store)
                {
                    Stores = new Dictionary<Expression<Func<Data, object>>, FieldStorage>
                            {
                                {
                                    x => x.Body, FieldStorage.Yes
                                },
                                {
                                    x => x.WhitespaceAnalyzerField, FieldStorage.Yes
                                }
                            };
                }

                if (termVector)
                {
                    TermVectors = new Dictionary<Expression<Func<Data, object>>, FieldTermVector>
                                {
                                    {
                                        x => x.Body, FieldTermVector.Yes
                                    },
                                    {
                                        x => x.WhitespaceAnalyzerField, FieldTermVector.Yes
                                    }
                                };
                }
            }
        }

        private class OtherDataIndex : AbstractIndexCreationTask<DataWithIntegerId>
        {
            public OtherDataIndex()
            {
                Map = docs => from doc in docs
                              select new { doc.Body };

                Analyzers = new Dictionary<Expression<Func<DataWithIntegerId, object>>, string>
                {
                    {
                        x => x.Body,
                        typeof (StandardAnalyzer).FullName
                    }
                };

                TermVectors = new Dictionary<Expression<Func<DataWithIntegerId, object>>, FieldTermVector>
                {
                    {
                        x => x.Body, FieldTermVector.Yes
                    }
                };

            }
        }

        private class ComplexDataIndex : AbstractIndexCreationTask<ComplexData>
        {
            public ComplexDataIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Property,
                                  doc.Property.Body
                              };

                Index(x => x.Property.Body, FieldIndexing.Search);
            }
        }
    }
}
