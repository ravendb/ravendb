using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Bundles.MoreLikeThis
{
    public class MoreLikeThisTests : RavenTest
    {
        private class Transformer1 : AbstractTransformerCreationTask<Data>
        {
            public class Result
            {
                public string TransformedBody { get; set; }
            }

            public Transformer1()
            {
                TransformResults = results => from result in results
                                              select new
                                              {
                                                  TransformedBody = result.Body + "123"
                                              };
            }
        }

        private class Transformer2 : AbstractTransformerCreationTask<Data>
        {
            public class Result
            {
                public string TransformedBody { get; set; }

                public string Name { get; set; }
            }

            public Transformer2()
            {
                TransformResults = results => from result in results
                                              let _ = Include<Person>("people/1")
                                              select new
                                              {
                                                  TransformedBody = result.Body + "321",
                                                  Name = LoadDocument<Person>("people/1").Name
                                              };
            }
        }

        private readonly IDocumentStore store;

        public MoreLikeThisTests()
        {
            store = NewDocumentStore();
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

        public void TransformersShouldWorkWithMoreLikeThis1()
        {
            string id;

            new Transformer1().Execute(store);

            using (var session = store.OpenSession())
            {
                new DataIndex(true, false).Execute(store);

                var list = GetDataList();
                list.ForEach(session.Store);
                session.SaveChanges();

                id = session.Advanced.GetDocumentId(list.First());
                WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Transformer1, Transformer1.Result, DataIndex>(new MoreLikeThisQuery
                {
                    DocumentId = id,
                    Fields = new[] { "Body" }
                });

                Assert.NotEmpty(list);
                foreach (var result in list)
                {
                    Assert.NotEmpty(result.TransformedBody);
                    Assert.True(result.TransformedBody.EndsWith("123"));
                }
            }
        }

        public void TransformersShouldWorkWithMoreLikeThis2()
        {
            string id;

            new Transformer2().Execute(store);

            using (var session = store.OpenSession())
            {
                new DataIndex(true, false).Execute(store);

                session.Store(new Person { Name = "Name1" });

                var list = GetDataList();
                list.ForEach(session.Store);
                session.SaveChanges();

                id = session.Advanced.GetDocumentId(list.First());
                WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Transformer2, Transformer2.Result, DataIndex>(new MoreLikeThisQuery
                {
                    DocumentId = id,
                    Fields = new[] { "Body" }
                });

                Assert.NotEmpty(list);
                foreach (var result in list)
                {
                    Assert.NotEmpty(result.TransformedBody);
                    Assert.True(result.TransformedBody.EndsWith("321"));
                    Assert.Equal("Name1", result.Name);
                }

                var numberOfRequests = session.Advanced.NumberOfRequests;
                var person = session.Load<Person>("people/1");
                Assert.NotNull(person);
                Assert.Equal("Name1", person.Name);
                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
            }
        }

        public void IncludesShouldWorkWithMoreLikeThis()
        {
            string id;

            new Transformer2().Execute(store);

            using (var session = store.OpenSession())
            {
                new DataIndex(true, false).Execute(store);

                session.Store(new { Name = "Name1" }, "test");

                var list = GetDataList();
                list.ForEach(session.Store);
                session.SaveChanges();

                id = session.Advanced.GetDocumentId(list.First());
                WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Data, DataIndex>(new MoreLikeThisQuery
                {
                    DocumentId = id,
                    Fields = new[] { "Body" },
                    Includes = new[] { "Body" }
                });

                Assert.NotEmpty(list);

                var numberOfRequests = session.Advanced.NumberOfRequests;
                var person = session.Load<dynamic>("test");
                Assert.NotNull(person);
                Assert.Equal("Name1", person.Name);
                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
            }
        }

        [Fact]
        public void CanGetResultsUsingTermVectors()
        {
            string id;

            using (var session = store.OpenSession())
            {
                new DataIndex(true, false).Execute(store);

                var list = GetDataList();
                list.ForEach(session.Store);
                session.SaveChanges();

                id = session.Advanced.GetDocumentId(list.First());
                WaitForIndexing(store);
            }

            AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(id);
        }

        [Fact]
        public async Task CanGetResultsUsingTermVectorsAsync()
        {
            string id;

            using (var session = store.OpenSession())
            {
                new DataIndex(true, false).Execute(store);

                var list = GetDataList();
                list.ForEach(session.Store);
                session.SaveChanges();

                id = session.Advanced.GetDocumentId(list.First());
                WaitForIndexing(store);
            }

            await AssetMoreLikeThisHasMatchesForAsync<Data, DataIndex>(id);
        }

        [Fact]
        public void CanGetResultsUsingStorage()
        {
            string id;

            using (var session = store.OpenSession())
            {
                new DataIndex(false, true).Execute(store);

                var list = GetDataList();
                list.ForEach(session.Store);
                session.SaveChanges();

                id = session.Advanced.GetDocumentId(list.First());
                WaitForIndexing(store);
            }

            AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(id);
        }

        [Fact]
        public void CanGetResultsUsingTermVectorsAndStorage()
        {
            string id;

            using (var session = store.OpenSession())
            {
                new DataIndex(true, true).Execute(store);

                var list = GetDataList();
                list.ForEach(session.Store);
                session.SaveChanges();

                id = session.Advanced.GetDocumentId(list.First());
                WaitForIndexing(store);
            }

            AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(id);
        }

        [Fact]
        public void CanCompareDocumentsWithIntegerIdentifiers()
        {
            string id;

            using (var session = store.OpenSession())
            {
                new OtherDataIndex().Execute(store);

                var dataQueriedFor = new DataWithIntegerId { Id = 123, Body = "This is a test. Isn't it great? I hope I pass my test!" };

                var list = new List<DataWithIntegerId>
                {
                    dataQueriedFor,
                    new DataWithIntegerId {Id = 234, Body = "I have a test tomorrow. I hate having a test"},
                    new DataWithIntegerId {Id = 3456, Body = "Cake is great."},
                    new DataWithIntegerId {Id = 3457, Body = "This document has the word test only once"},
                    new DataWithIntegerId {Id = 3458, Body = "test"},
                    new DataWithIntegerId {Id = 3459, Body = "test"},
                };
                list.ForEach(session.Store);
                session.SaveChanges();

                id = session.Advanced.GetDocumentId(dataQueriedFor);

                WaitForIndexing(store);
            }

            Console.WriteLine("Test: '{0}'", id);
            AssetMoreLikeThisHasMatchesFor<DataWithIntegerId, OtherDataIndex>(id);

            id = id.ToLower();
            Console.WriteLine("Test with lowercase: '{0}'", id);
            AssetMoreLikeThisHasMatchesFor<DataWithIntegerId, OtherDataIndex>(id);
        }

        [Fact]
        public void CanGetResultsWhenIndexHasSlashInIt()
        {
            const string key = "datas/1";

            using (var session = store.OpenSession())
            {
                new DataIndex().Execute(store);

                var list = GetDataList();
                list.ForEach(session.Store);
                session.SaveChanges();
                WaitForIndexing(store);
            }

            AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(key);
        }

        [Fact]
        public void Query_On_Document_That_Does_Not_Have_High_Enough_Word_Frequency()
        {
            const string key = "datas/4";

            using (var session = store.OpenSession())
            {
                new DataIndex().Execute(store);

                var list = GetDataList();
                list.ForEach(session.Store);
                session.SaveChanges();
                WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Data, DataIndex>(new MoreLikeThisQuery
                {
                    DocumentId = key,
                    Fields = new[] { "Body" }
                });
                WaitForIndexing(store);

                Assert.Empty(list);
            }
        }

        [Fact]
        public void Test_With_Lots_Of_Random_Data()
        {
            var key = "datas/1";
            using (var session = store.OpenSession())
            {
                new DataIndex().Execute(store);

                for (var i = 0; i < 100; i++)
                {
                    var data = new Data { Body = GetLorem(200) };
                    session.Store(data);
                }
                session.SaveChanges();

                WaitForIndexing(store);
            }

            AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(key);
        }

        [Fact]
        public void Do_Not_Pass_FieldNames()
        {
            var key = "datas/1";
            using (var session = store.OpenSession())
            {
                new DataIndex().Execute(store);

                for (var i = 0; i < 10; i++)
                {
                    var data = new Data { Body = "Body" + i, WhitespaceAnalyzerField = "test test" };
                    session.Store(data);
                }
                session.SaveChanges();

                WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Data, DataIndex>(key);

                Assert.NotEmpty(list);
            }
        }

        [Fact]
        public void Each_Field_Should_Use_Correct_Analyzer()
        {
            var key = "datas/1";
            using (var session = store.OpenSession())
            {
                new DataIndex().Execute(store);

                for (var i = 0; i < 10; i++)
                {
                    var data = new Data { WhitespaceAnalyzerField = "bob@hotmail.com hotmail" };
                    session.Store(data);
                }
                session.SaveChanges();

                WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Data, DataIndex>(key);

                Assert.Empty(list);
            }

            key = "datas/11";
            using (var session = store.OpenSession())
            {
                new DataIndex().Execute(store);

                for (var i = 0; i < 10; i++)
                {
                    var data = new Data { WhitespaceAnalyzerField = "bob@hotmail.com bob@hotmail.com" };
                    session.Store(data);
                }
                session.SaveChanges();

                WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Data, DataIndex>(key);

                Assert.NotEmpty(list);
            }
        }

        [Fact]
        public void Can_Use_Min_Doc_Freq_Param()
        {
            const string key = "datas/1";

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

                WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Data, DataIndex>(new MoreLikeThisQuery
                {
                    DocumentId = key,
                    Fields = new[] { "Body" },
                    MinimumDocumentFrequency = 2
                });

                Assert.NotEmpty(list);
            }
        }

        [Fact]
        public void Can_Use_Boost_Param()
        {
            const string key = "datas/1";

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

                WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Data, DataIndex>(
                    new MoreLikeThisQuery
                    {
                        DocumentId = key,
                        Fields = new[] { "Body" },
                        MinimumWordLength = 3,
                        MinimumDocumentFrequency = 1,
                        Boost = true
                    });

                Assert.NotEqual(0, list.Count());
                Assert.Equal("I have a test tomorrow.", list[0].Body);
            }
        }

        [Fact]
        public void Can_Use_Stop_Words()
        {
            const string key = "datas/1";

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

                session.Store(new StopWordsSetup { Id = "Config/Stopwords", StopWords = new List<string> { "I", "A", "Be" } });

                session.SaveChanges();

                WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Data, DataIndex>(new MoreLikeThisQuery
                {
                    DocumentId = key,
                    StopWordsDocumentId = "Config/Stopwords",
                    MinimumDocumentFrequency = 1
                });

                Assert.Equal(5, list.Count());
            }
        }

        [Fact]
        public void CanMakeDynamicDocumentQueries()
        {
            new DataIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                var list = GetDataList();
                list.ForEach(session.Store);

                session.SaveChanges();
            }

            WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<Data, DataIndex>(
                    new MoreLikeThisQuery
                    {
                        Document = "{ \"Body\": \"A test\" }",
                        Fields = new[] { "Body" },
                        MinimumTermFrequency = 1,
                        MinimumDocumentFrequency = 1
                    });

                Assert.Equal(7, list.Count());
            }
        }

        private void AssetMoreLikeThisHasMatchesFor<T, TIndex>(string documentKey) where TIndex : AbstractIndexCreationTask, new()
        {
            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<T, TIndex>(new MoreLikeThisQuery
                {
                    DocumentId = documentKey,
                    Fields = new[] { "Body" }
                });

                Assert.NotEmpty(list);
            }
        }

        private async Task AssetMoreLikeThisHasMatchesForAsync<T, TIndex>(string documentKey) where TIndex : AbstractIndexCreationTask, new()
        {
            using (var session = store.OpenAsyncSession())
            {
                var list = await session.Advanced.MoreLikeThisAsync<T, TIndex>(new MoreLikeThisQuery
                {
                    DocumentId = documentKey,
                    Fields = new[] { "Body" }
                });

                Assert.NotEmpty(list);
            }
        }

        private void InsertData()
        {
            using (var session = store.OpenSession())
            {
                new DataIndex().Execute(store);

                var list = new List<Data>
                {
                    new Data {Body = "This is a test. Isn't it great?"},
                    new Data {Body = "I have a test tomorrow. I hate having a test"},
                    new Data {Body = "Cake is great."},
                    new Data {Body = "test"},
                    new Data {Body = "test"},
                    new Data {Body = "test"},
                    new Data {Body = "test"},
                    new Data {Body = "test"}
                };

                foreach (var data in list)
                {
                    session.Store(data);
                }

                session.SaveChanges();

                //Ensure non stale index
                var testObj = session.Query<Data, DataIndex>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Id == list[0].Id).SingleOrDefault();
            }
        }

        public class Data
        {
            public string Id { get; set; }
            public string Body { get; set; }
            public string WhitespaceAnalyzerField { get; set; }
        }

        public class DataWithIntegerId
        {
            public long Id;
            public string Body { get; set; }
        }

        public class DataIndex : AbstractIndexCreationTask<Data>
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

        public class OtherDataIndex : AbstractIndexCreationTask<DataWithIntegerId>
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
    }
}
