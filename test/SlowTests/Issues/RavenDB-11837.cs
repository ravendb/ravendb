using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11837 : RavenTestBase
    {
        [Fact]
        public void CanApplyCounterToAnotherDocument()
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
                                    var newDocId = 'newDocId';
                                    put(newDocId, doc);
                                    var countByDay = doc.CountByDay;
                                    for(var i in countByDay) {
                                        incrementCounter(newDocId, i, countByDay[i])
                                    }
                                  }"
                     })).WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>(testDoc.Id);
                    var mdCounters = session.Advanced.GetCountersFor(doc);
                    Assert.Null(mdCounters);

                    doc = session.Load<TestDoc>("newDocId");
                    mdCounters = session.Advanced.GetCountersFor(doc);
                    Assert.Equal(testDoc.CountByDay.Count, mdCounters.Count);
                    foreach (var c in testDoc.CountByDay.Keys)
                    {
                        Assert.True(mdCounters.Contains(c));
                    }
                }
            }
        }

        [Fact]
        public void CanDeleteCounterFromAnotherDocument()
        {
            using (var store = GetDocumentStore())
            {
                var testDoc1 = new TestDoc
                {
                    Name = "Aviv"
                };

                var testDoc2 = new TestDoc
                {
                    Name = "Grisha"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(testDoc1);
                    session.CountersFor(testDoc1).Increment("likes", 100);
                    session.CountersFor(testDoc1).Increment("dislikes", 200);
                    session.CountersFor(testDoc1).Increment("downloads", 600);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(testDoc2);
                    session.CountersFor(testDoc2).Increment("likes", 1000);
                    session.CountersFor(testDoc2).Increment("dislikes", 2000);
                    session.CountersFor(testDoc2).Increment("downloads", 6000);
                    session.SaveChanges();
                }

                store.Operations
                    .Send(new PatchOperation(testDoc1.Id, null, new PatchRequest
                    {
                        Script = @" deleteCounter(args.id, 'dislikes')",
                        Values = new Dictionary<string, object>
                        {
                            {"id", testDoc2.Id}
                        }
                    }));

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>(testDoc1.Id);
                    var mdCounters = session.Advanced.GetCountersFor(doc);

                    Assert.Equal(3, mdCounters.Count);

                    doc = session.Load<TestDoc>(testDoc2.Id);
                    mdCounters = session.Advanced.GetCountersFor(doc);

                    Assert.Equal(2, mdCounters.Count);
                    var counters = session.CountersFor(doc).GetAll();
                    Assert.Equal(1000, counters["likes"]);
                    Assert.Equal(6000, counters["downloads"]);
                }
            }
        }

        [Fact]
        public void ThrowIfCounterDoesntExist()
        {
            using (var store = GetDocumentStore())
            {
                var testDoc1 = new TestDoc
                {
                    Name = "Aviv"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(testDoc1);
                    session.SaveChanges();
                }

                Assert.Throws<DocumentDoesNotExistException>(() =>
                {
                    store.Operations
                        .Send(new PatchOperation(testDoc1.Id, null, new PatchRequest
                        {
                            Script = @" incrementCounter('stam-id', 'dislikes', 100)"
                        }));
                });

                Assert.Throws<DocumentDoesNotExistException>(() =>
                {
                    store.Operations
                        .Send(new PatchOperation(testDoc1.Id, null, new PatchRequest
                        {
                            Script = @" deleteCounter('stam-id', 'dislikes')"
                        }));
                });
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
