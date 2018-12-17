using System.Collections.Generic;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11824 : RavenTestBase
    {

        [Fact]
        public void AfterIncrementingMultipleCountersByScriptMetadataShouldHaveAllCountersNames()
        {
            using (var store = GetDocumentStore())
            {
                var testDoc = new TestDoc
                {
                    CountByDay = new Dictionary<string, long>
                    {
                        {"2018-08-01T00:00:00.0000000", 120},
                        {"2018-08-02T00:00:00.0000000", 120},
                        {"2018-08-03T00:00:00.0000000", 40},
                        {"2018-08-04T00:00:00.0000000", 65},
                        {"2018-08-06T00:00:00.0000000", 35},
                        {"2018-08-07T00:00:00.0000000", 120},
                        {"2018-08-08T00:00:00.0000000", 120},
                        {"2018-08-09T00:00:00.0000000", 120},
                        {"2018-08-10T00:00:00.0000000", 120},
                        {"2018-08-11T00:00:00.0000000", 120},
                        {"2018-08-12T00:00:00.0000000", 120},
                        {"2018-08-13T00:00:00.0000000", 117},
                        {"2018-08-14T00:00:00.0000000", 120},
                        {"2018-08-15T00:00:00.0000000", 120},
                        {"2018-08-16T00:00:00.0000000", 120},
                        {"2018-08-17T00:00:00.0000000", 120},
                        {"2018-08-18T00:00:00.0000000", 120},
                        {"2018-08-19T00:00:00.0000000", 120},
                        {"2018-08-20T00:00:00.0000000", 120},
                        {"2018-08-21T00:00:00.0000000", 120},
                        {"2018-08-22T00:00:00.0000000", 120},
                        {"2018-08-23T00:00:00.0000000", 120},
                        {"2018-08-24T00:00:00.0000000", 121},
                        {"2018-08-25T00:00:00.0000000", 120},
                        {"2018-08-26T00:00:00.0000000", 120},
                        {"2018-08-27T00:00:00.0000000", 80}
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(testDoc);
                    session.SaveChanges();
                }

                store.Operations
                     .Send(new PatchByQueryOperation(new IndexQuery
                     {
                         Query = @"from TestDocs as doc
                                  update
                                  {
                                    var countByDay = doc.CountByDay;
                                    for(var i in countByDay) {
                                        incrementCounter(this, i, countByDay[i])
                                    }
                                  }"
                     })).WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>(testDoc.Id);
                    var mdCounters = session.Advanced.GetCountersFor(doc);

                    Assert.Equal(testDoc.CountByDay.Count, mdCounters.Count);
                    foreach (var c in testDoc.CountByDay.Keys)
                    {
                        Assert.True(mdCounters.Contains(c));
                    }
                }
            }
        }

        [Fact]
        public void CanModifyDocAndIncrementCounterInSameScript()
        {
            using (var store = GetDocumentStore())
            {
                var testDoc = new TestDoc
                {
                    CountByDay = new Dictionary<string, long>
                    {
                        {"2018-08-01T00:00:00.0000000", 120},
                        {"2018-08-03T00:00:00.0000000", 40}
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(testDoc);
                    session.SaveChanges();
                }

                store.Operations
                     .Send(new PatchByQueryOperation(new IndexQuery
                     {
                         Query = @"from TestDocs as doc
                                  update
                                  {
                                    var countByDay = doc.CountByDay;
                                    for(var i in countByDay) {
                                        incrementCounter(this, i, countByDay[i])
                                    };
                                    this.Name = 'Grisha';
                                  }"
                     })).WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>(testDoc.Id);
                    var mdCounters = session.Advanced.GetCountersFor(doc);

                    Assert.Equal(2, mdCounters.Count);
                    Assert.True(mdCounters.Contains("2018-08-01T00:00:00.0000000"));
                    Assert.True(mdCounters.Contains("2018-08-03T00:00:00.0000000"));

                    var counters = session.CountersFor(doc).GetAll();
                    Assert.Equal(2, counters.Count);
                    Assert.Equal(120, counters["2018-08-01T00:00:00.0000000"]);
                    Assert.Equal(40, counters["2018-08-03T00:00:00.0000000"]);

                    Assert.Equal("Grisha", doc.Name);
                }
            }
        }


        [Fact]
        public void DeletingCountersViaScriptShouldRemoveDeletedCountersNamesFromMetadata()
        {
            using (var store = GetDocumentStore())
            {
                var testDoc = new TestDoc
                {
                    Name = "Grisha"
                };


                using (var session = store.OpenSession())
                {
                    session.Store(testDoc);
                    session.CountersFor(testDoc).Increment("likes", 100);
                    session.CountersFor(testDoc).Increment("dislikes", 200);
                    session.CountersFor(testDoc).Increment("downloads", 600);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>(testDoc.Id);
                    var mdCounters = session.Advanced.GetCountersFor(doc);

                    Assert.Equal(3, mdCounters.Count);


                }

                store.Operations
                     .Send(new PatchByQueryOperation(new IndexQuery
                     {
                         Query = @"from TestDocs as doc
                                  update
                                  {
                                    deleteCounter(this, 'likes');
                                    deleteCounter(this, 'downloads');
                                  }"
                     })).WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>(testDoc.Id);
                    var mdCounters = session.Advanced.GetCountersFor(doc);

                    Assert.Equal(1, mdCounters.Count);
                    Assert.True(mdCounters.Contains("dislikes"));

                    var md = session.Advanced.GetMetadataFor(doc);
                    Assert.True(md.ContainsKey(Constants.Documents.Metadata.Counters));
                }

                store.Operations
                    .Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = @"from TestDocs as doc
                                  update
                                  {
                                    deleteCounter(this, 'dislikes');
                                  }"
                    })).WaitForCompletion();


                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>(testDoc.Id);
                    var mdCounters = session.Advanced.GetCountersFor(doc);

                    Assert.Null(mdCounters);

                    var md = session.Advanced.GetMetadataFor(doc);
                    Assert.False(md.ContainsKey(Constants.Documents.Metadata.Counters));
                }
            }
        }

        private class TestDoc
        {
            public string Name { get; set; }
            public string Id { get; set; }
            public Dictionary<string, long> CountByDay { get; set; }
        }
    }
}
