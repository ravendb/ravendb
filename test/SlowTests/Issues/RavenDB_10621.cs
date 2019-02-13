using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10621 : RavenTestBase
    {
        [Fact]
        public void ShouldNotErrorIndexOnInvalidProgramException()
        {
            // if this test fails it's very likely the following issue got fixed: https://github.com/dotnet/coreclr/issues/14672

            using (var store = GetDocumentStore())
            {
                new BigIndexOutput().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ErroringDocument
                    {
                        NumVals = { { "Value001", 2.0 } }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var happyQuery = session.Query<ErroringDocument, BigIndexOutput>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(1, happyQuery.Count);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(new BigIndexOutput().IndexName));

                Assert.Equal(IndexState.Normal, indexStats.State);
            }
        }

        [Fact]
        public void ShouldNotErrorIndexByInvalidProgramExceptionWhenUsingDictionary()
        {
            using (var store = GetDocumentStore())
            {
                new BigIndexOutput_WithDictionaryUsage().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ErroringDocument
                    {
                        NumVals = { { "Value001", 2.0 } }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<ErroringDocument, BigIndexOutput_WithDictionaryUsage>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }

        private class BigIndexOutput : AbstractIndexCreationTask<ErroringDocument>
        {
            public BigIndexOutput()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Value001 = doc.NumVals.ContainsKey("Value001") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value002 = doc.NumVals.ContainsKey("Value002") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value003 = doc.NumVals.ContainsKey("Value003") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value004 = doc.NumVals.ContainsKey("Value004") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value005 = doc.NumVals.ContainsKey("Value005") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value006 = doc.NumVals.ContainsKey("Value006") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value007 = doc.NumVals.ContainsKey("Value007") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value008 = doc.NumVals.ContainsKey("Value008") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value009 = doc.NumVals.ContainsKey("Value009") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value010 = doc.NumVals.ContainsKey("Value010") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),

                                  Value011 = doc.NumVals.ContainsKey("Value011") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value012 = doc.NumVals.ContainsKey("Value012") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value013 = doc.NumVals.ContainsKey("Value013") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value014 = doc.NumVals.ContainsKey("Value014") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value015 = doc.NumVals.ContainsKey("Value015") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value016 = doc.NumVals.ContainsKey("Value016") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value017 = doc.NumVals.ContainsKey("Value017") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value018 = doc.NumVals.ContainsKey("Value018") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value019 = doc.NumVals.ContainsKey("Value019") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value020 = doc.NumVals.ContainsKey("Value020") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),

                                  Value021 = doc.NumVals.ContainsKey("Value021") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value022 = doc.NumVals.ContainsKey("Value022") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value023 = doc.NumVals.ContainsKey("Value023") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value024 = doc.NumVals.ContainsKey("Value024") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value025 = doc.NumVals.ContainsKey("Value025") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value026 = doc.NumVals.ContainsKey("Value026") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value027 = doc.NumVals.ContainsKey("Value027") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value028 = doc.NumVals.ContainsKey("Value028") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value029 = doc.NumVals.ContainsKey("Value029") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value030 = doc.NumVals.ContainsKey("Value030") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),

                                  Value041 = doc.NumVals.ContainsKey("Value041") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value042 = doc.NumVals.ContainsKey("Value042") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value043 = doc.NumVals.ContainsKey("Value043") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value044 = doc.NumVals.ContainsKey("Value044") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value045 = doc.NumVals.ContainsKey("Value045") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value046 = doc.NumVals.ContainsKey("Value046") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value047 = doc.NumVals.ContainsKey("Value047") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value048 = doc.NumVals.ContainsKey("Value048") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value049 = doc.NumVals.ContainsKey("Value049") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value050 = doc.NumVals.ContainsKey("Value050") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),

                                  Value061 = doc.NumVals.ContainsKey("Value061") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value062 = doc.NumVals.ContainsKey("Value062") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value063 = doc.NumVals.ContainsKey("Value063") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value064 = doc.NumVals.ContainsKey("Value064") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value065 = doc.NumVals.ContainsKey("Value065") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value066 = doc.NumVals.ContainsKey("Value066") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value067 = doc.NumVals.ContainsKey("Value067") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value068 = doc.NumVals.ContainsKey("Value068") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value069 = doc.NumVals.ContainsKey("Value069") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value070 = doc.NumVals.ContainsKey("Value070") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),

                                  Value071 = doc.NumVals.ContainsKey("Value071") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value072 = doc.NumVals.ContainsKey("Value072") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value073 = doc.NumVals.ContainsKey("Value073") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value074 = doc.NumVals.ContainsKey("Value074") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value075 = doc.NumVals.ContainsKey("Value075") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value076 = doc.NumVals.ContainsKey("Value076") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value077 = doc.NumVals.ContainsKey("Value077") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value078 = doc.NumVals.ContainsKey("Value078") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value079 = doc.NumVals.ContainsKey("Value079") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                                  Value080 = doc.NumVals.ContainsKey("Value080") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null),
                              };
            }
        }

        private class BigIndexOutput_WithDictionaryUsage : AbstractIndexCreationTask<ErroringDocument>
        {
            public BigIndexOutput_WithDictionaryUsage()
            {
                Map = docs => from doc in docs
                              select new Dictionary<string, object>()
                              {
                                  { "Value001",  doc.NumVals.ContainsKey("Value001") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value002",  doc.NumVals.ContainsKey("Value002") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value003",  doc.NumVals.ContainsKey("Value003") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value004",  doc.NumVals.ContainsKey("Value004") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value005",  doc.NumVals.ContainsKey("Value005") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value006",  doc.NumVals.ContainsKey("Value006") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value007",  doc.NumVals.ContainsKey("Value007") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value008",  doc.NumVals.ContainsKey("Value008") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value009",  doc.NumVals.ContainsKey("Value009") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value010",  doc.NumVals.ContainsKey("Value010") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },

                                  { "Value011",  doc.NumVals.ContainsKey("Value011") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value012",  doc.NumVals.ContainsKey("Value012") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value013",  doc.NumVals.ContainsKey("Value013") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value014",  doc.NumVals.ContainsKey("Value014") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value015",  doc.NumVals.ContainsKey("Value015") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value016",  doc.NumVals.ContainsKey("Value016") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value017",  doc.NumVals.ContainsKey("Value017") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value018",  doc.NumVals.ContainsKey("Value018") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value019",  doc.NumVals.ContainsKey("Value019") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value020",  doc.NumVals.ContainsKey("Value020") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },

                                  { "Value021",  doc.NumVals.ContainsKey("Value021") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value022",  doc.NumVals.ContainsKey("Value022") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value023",  doc.NumVals.ContainsKey("Value023") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value024",  doc.NumVals.ContainsKey("Value024") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value025",  doc.NumVals.ContainsKey("Value025") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value026",  doc.NumVals.ContainsKey("Value026") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value027",  doc.NumVals.ContainsKey("Value027") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value028",  doc.NumVals.ContainsKey("Value028") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value029",  doc.NumVals.ContainsKey("Value029") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value030",  doc.NumVals.ContainsKey("Value030") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },

                                  { "Value041",  doc.NumVals.ContainsKey("Value041") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value042",  doc.NumVals.ContainsKey("Value042") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value043",  doc.NumVals.ContainsKey("Value043") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value044",  doc.NumVals.ContainsKey("Value044") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value045",  doc.NumVals.ContainsKey("Value045") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value046",  doc.NumVals.ContainsKey("Value046") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value047",  doc.NumVals.ContainsKey("Value047") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value048",  doc.NumVals.ContainsKey("Value048") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value049",  doc.NumVals.ContainsKey("Value049") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value050",  doc.NumVals.ContainsKey("Value050") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },

                                  { "Value061",  doc.NumVals.ContainsKey("Value061") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value062",  doc.NumVals.ContainsKey("Value062") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value063",  doc.NumVals.ContainsKey("Value063") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value064",  doc.NumVals.ContainsKey("Value064") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value065",  doc.NumVals.ContainsKey("Value065") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value066",  doc.NumVals.ContainsKey("Value066") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value067",  doc.NumVals.ContainsKey("Value067") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value068",  doc.NumVals.ContainsKey("Value068") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value069",  doc.NumVals.ContainsKey("Value069") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value070",  doc.NumVals.ContainsKey("Value070") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },

                                  { "Value071",  doc.NumVals.ContainsKey("Value071") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value072",  doc.NumVals.ContainsKey("Value072") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value073",  doc.NumVals.ContainsKey("Value073") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value074",  doc.NumVals.ContainsKey("Value074") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value075",  doc.NumVals.ContainsKey("Value075") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value076",  doc.NumVals.ContainsKey("Value076") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value077",  doc.NumVals.ContainsKey("Value077") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value078",  doc.NumVals.ContainsKey("Value078") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value079",  doc.NumVals.ContainsKey("Value079") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                                  { "Value080",  doc.NumVals.ContainsKey("Value080") && doc.NumVals["Value001"] != null ? (doc.NumVals["Value001"]).Value : ((double?)null) },
                              };
            }
        }

        private class ErroringDocument
        {
            public Dictionary<string, double?> NumVals = new Dictionary<string, double?>();
        }
    }
}
