// -----------------------------------------------------------------------
//  <copyright file="GroupByAndDocumentId.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class GroupByAndDocumentId : RavenTestBase
    {
        private class Client
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public IList<ImportStatusMessage> ImportStatuses { get; set; }
        }

        private class ImportStatusMessage
        {
            public DateTime TimeStamp { get; set; }
            public ImportStatus Status { get; set; }
        }

        private enum ImportStatus
        {
            Complete,
            Running,
            Failed,
            Waiting,
            NoReport
        }

        [Fact]
        public void Test1()
        {
            DoTest<Client_ImportSummaryByDate_1>();
        }

        [Fact]
        public void Test2()
        {
            DoTest<Client_ImportSummaryByDate_2>();
        }

        [Fact]
        public void Test3()
        {
            DoTest<Client_ImportSummaryByDate_3>();
        }

        [Fact]
        public void Test4()
        {
            DoTest<Client_ImportSummaryByDate_4>();
        }

        private void DoTest<TIndex>()
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new TIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new Client
                    {
                        Name = "A",
                        ImportStatuses = new List<ImportStatusMessage>
                        {
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Waiting,
                                TimeStamp = new DateTime(2013, 1, 1, 0, 0, 0)
                            },
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Running,
                                TimeStamp = new DateTime(2013, 1, 1, 1, 0, 0)
                            },
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Complete,
                                TimeStamp = new DateTime(2013, 1, 1, 2, 0, 0)
                            },
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Waiting,
                                TimeStamp = new DateTime(2013, 2, 2, 0, 0, 0)
                            },
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Running,
                                TimeStamp = new DateTime(2013, 2, 2, 1, 0, 0)
                            },
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Complete,
                                TimeStamp = new DateTime(2013, 2, 2, 2, 0, 0)
                            },
                        }
                    });

                    session.Store(new Client
                    {
                        Name = "B",
                        ImportStatuses = new List<ImportStatusMessage>
                        {
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Waiting,
                                TimeStamp = new DateTime(2013, 1, 1, 0, 0, 0)
                            },
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Running,
                                TimeStamp = new DateTime(2013, 1, 1, 1, 0, 0)
                            },
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Complete,
                                TimeStamp = new DateTime(2013, 1, 1, 2, 0, 0)
                            },
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Waiting,
                                TimeStamp = new DateTime(2013, 2, 2, 0, 0, 0)
                            },
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Running,
                                TimeStamp = new DateTime(2013, 2, 2, 1, 0, 0)
                            },
                            new ImportStatusMessage
                            {
                                Status = ImportStatus.Complete,
                                TimeStamp = new DateTime(2013, 2, 2, 2, 0, 0)
                            },
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var results = Queryable.OrderBy(session.Query<ImportSummary, TIndex>(), summary => summary.Date)
                        .ToArray();

                    Assert.Equal(2, results.Length);

                    Assert.Equal(2, results[0].Count);
                    Assert.Equal(ImportStatus.Complete, results[0].Status);
                    Assert.Equal(new DateTime(2013, 1, 1), results[0].Date);

                    Assert.Equal(2, results[1].Count);
                    Assert.Equal(ImportStatus.Complete, results[1].Status);
                    Assert.Equal(new DateTime(2013, 2, 2), results[1].Date);
                }
            }
        }



        // Pass
        private class Client_ImportSummaryByDate_1 : AbstractIndexCreationTask<Client, ImportSummary>
        {
            public Client_ImportSummaryByDate_1()
            {
                Map = clients => clients.SelectMany(x => x.ImportStatuses, (x, y) => new { x.Id, y.Status, y.TimeStamp })
                                        .GroupBy(x => new { x.Id, x.TimeStamp.Date })
                                        .Select(g => g.OrderBy(x => x.TimeStamp).Last())
                                        .Select(x => new
                                        {
                                            x.Status,
                                            x.TimeStamp.Date,
                                            Count = 1
                                        });

                Reduce = results => from result in results
                                    group result by new { result.Status, result.Date }
                                    into g
                                    select new
                                    {
                                        g.Key.Status,
                                        g.Key.Date,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        // Fail
        private class Client_ImportSummaryByDate_2 : AbstractIndexCreationTask<Client, ImportSummary>
        {
            public Client_ImportSummaryByDate_2()
            {
                Map = clients => clients.SelectMany(x => x.ImportStatuses, (x, y) => new { x.Id, y.Status, y.TimeStamp })
                                        .GroupBy(x => new { x.Id, x.TimeStamp.Date })
                                        .Select(g => new
                                        {
                                            g.OrderBy(x => x.TimeStamp).Last().Status,
                                            g.Key.Date,
                                            Count = 1
                                        });

                Reduce = results => from result in results
                                    group result by new { result.Status, result.Date }
                                    into g
                                    select new
                                    {
                                        g.Key.Status,
                                        g.Key.Date,
                                        Count = g.Sum(x => x.Count)
                                    };

            }
        }

        // Fail
        private class Client_ImportSummaryByDate_3 : AbstractIndexCreationTask<Client, ImportSummary>
        {
            public Client_ImportSummaryByDate_3()
            {
                Map = clients => from client in clients
                                 from status in client.ImportStatuses
                                 group status by new { client.Id, status.TimeStamp.Date }
                                 into g
                                 let z = g.OrderBy(x => x.TimeStamp).Last()
                                 select new
                                 {
                                     z.Status,
                                     g.Key.Date,
                                     Count = 1
                                 };

                Reduce = results => from result in results
                                    group result by new { result.Status, result.Date }
                                    into g
                                    select new
                                    {
                                        g.Key.Status,
                                        g.Key.Date,
                                        Count = g.Sum(x => x.Count)
                                    };

            }
        }

        // Fail
        private class Client_ImportSummaryByDate_4 : AbstractIndexCreationTask<Client, ImportSummary>
        {
            public Client_ImportSummaryByDate_4()
            {
                Map = clients => from client in clients
                                 from status in client.ImportStatuses
                                 group status by new { client.Id, status.TimeStamp.Date }
                                 into g
                                 select new
                                 {
                                     g.OrderBy(x => x.TimeStamp).Last().Status,
                                     g.Key.Date,
                                     Count = 1
                                 };

                Reduce = results => from result in results
                                    group result by new { result.Status, result.Date }
                                    into g
                                    select new
                                    {
                                        g.Key.Status,
                                        g.Key.Date,
                                        Count = g.Sum(x => x.Count)
                                    };

            }
        }

        private class ImportSummary
        {
            public ImportStatus Status { get; set; }
            public DateTime Date { get; set; }
            public int Count { get; set; }
        }
    }
}