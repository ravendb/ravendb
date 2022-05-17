using Tests.Infrastructure;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13503 : RavenTestBase
    {
        public RavenDB_13503(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void IndexProjectionShouldSupportMathematicalAdditionOfNumberProperties(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Document document;
                using (var session = store.OpenSession())
                {
                    document = new Document
                    {
                        Statistics = new DocumentStatistics
                        {
                            IntegerNumber1 = 3,
                            IntegerNumber2 = 2,
                            DecimalNumber1 = 3,
                            DecimalNumber2 = 2,
                            DoubleNumber1 = 300.33,
                            DoubleNumber2 = 400.32,
                            String = "Or"
                        }
                    };
                    session.Store(document);
                    session.SaveChanges();
                }

                new DocumentIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var projection = (from doc in session.Query<DocumentIndex.Result, DocumentIndex>()
                                .Customize(x => x.WaitForNonStaleResults())
                                      select new Result
                                      {
                                          IntegerSum = doc.IntegerNumber1 + doc.IntegerNumber2,
                                          IntegerDup = doc.IntegerNumber1 * doc.IntegerNumber2,
                                          DecimalSum = doc.DecimalNumber1 + doc.DecimalNumber2,
                                          DoubleSum = doc.DoubleNumber1 + doc.DoubleNumber2,
                                          DoubleAndStringSum = doc.String + doc.DoubleNumber2,
                                          DoubleAndIntSum = doc.IntegerNumber1 + doc.DoubleNumber2,
                                          StringSum = doc.String + doc.IntegerNumber2
                                      })
                        .Single();

                    Assert.Equal(5, projection.IntegerSum);
                    Assert.Equal(5, projection.DecimalSum);
                    Assert.Equal(6, projection.IntegerDup);
                    Assert.Equal(700.65, projection.DoubleSum);
                    Assert.Equal("Or2", projection.StringSum);
                    Assert.Equal(403.32, projection.DoubleAndIntSum);
                    Assert.Equal("Or400.32", projection.DoubleAndStringSum);
                }
            }
        }

        private class Result
        {
            public int IntegerSum { get; set; }
            public double DoubleSum { get; set; }
            public decimal DecimalSum { get; set; }
            public string StringSum { get; set; }
            public string DoubleAndStringSum { get; set; }
            public double DoubleAndIntSum { get; set; }
            public int IntegerDup { get; set; }
        }

        private class Document
        {
#pragma warning disable 649
            public string Id;
#pragma warning restore 649
            public DocumentStatistics Statistics;
        }

        private class DocumentStatistics
        {
            public int IntegerNumber1;
            public int IntegerNumber2;
            public decimal DecimalNumber1;
            public decimal DecimalNumber2;
            public double DoubleNumber1;
            public double DoubleNumber2;
            public string String;
        }

        private class DocumentIndex : AbstractIndexCreationTask<Document>
        {
            public class Result
            {
                public int IntegerNumber1;
                public int IntegerNumber2;
                public decimal DecimalNumber1;
                public decimal DecimalNumber2;
                public double DoubleNumber1;
                public double DoubleNumber2;
                public string String;
            }

            public DocumentIndex()
            {
                Map = docs => from doc in docs
                              select new Result
                              {
                                  IntegerNumber1 = doc.Statistics.IntegerNumber1,
                                  IntegerNumber2 = doc.Statistics.IntegerNumber2,
                                  DecimalNumber1 = doc.Statistics.DecimalNumber1,
                                  DecimalNumber2 = doc.Statistics.DecimalNumber2,
                                  DoubleNumber1 = doc.Statistics.DoubleNumber1,
                                  DoubleNumber2 = doc.Statistics.DoubleNumber2,
                                  String = doc.Statistics.String
                              };

                StoreAllFields(FieldStorage.Yes);
            }

        }
    }
}
