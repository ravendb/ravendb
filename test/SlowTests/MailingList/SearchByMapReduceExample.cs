// -----------------------------------------------------------------------
//  <copyright file="SearchByMapReduceExample.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class SearchByMapReduceExample : RavenTestBase
    {
        [Fact]
        public void GivenAListOfLogEntriesAndAPartialClientName_Search_Returns1Player()
        {
            // Act.
            using (var documentStore = CreateDocumentStore())
            {

                // Arrange.
                IList<LogEntries_Search.ReduceResult> result = Search(documentStore, "Jus");

                // Assert.
                Assert.NotNull(result);
                Assert.Equal(1, result.Count);
                Assert.True(result.First().ClientNames.Contains("[WD] Jussy"));
            }
        }

        [Fact]
        public void GivenAListOfLogEntriesAndAFullClientName_Search_Returns1Player()
        {
            // Act.
            using (var documentStore = CreateDocumentStore())
            {
                // Arrange.
                IList<LogEntries_Search.ReduceResult> result = Search(documentStore, "Jus");

                // Assert.
                Assert.NotNull(result);
                Assert.Equal(1, result.Count);
                Assert.True(result.First().ClientNames.Contains("[WD] Jussy"));
            }
        }

        [Fact]
        public void GivenAListOfLogEntriesAndAFullGuid_Search_Returns1PlayerWith3ClientNames()
        {
            // Act.
            using (var documentStore = CreateDocumentStore())
            {

                // Arrange.
                IList<LogEntries_Search.ReduceResult> result = Search(documentStore, "638867b0-82b9-11e1-b0c4-0800200c9a66");

                // Assert.
                Assert.NotNull(result);
                Assert.Equal(1, result.Count);
                Assert.Equal(new Guid("638867b0-82b9-11e1-b0c4-0800200c9a66"), result.First().ClientGuid);
            }
        }

        [Fact]
        public void GivenAListOfLogEntriesAndAPartialEndOfClientGuid_Search_Returns1PlayerWith3ClientNames()
        {
            // Act.
            using (var documentStore = CreateDocumentStore())
            {

                // Arrange.
                // NOTE: End guid chars for: 638867b0-82b9-11e1-b0c4-0800200c9a66
                IList<LogEntries_Search.ReduceResult> result = Search(documentStore, "0800200c9a66");

                // Assert.
                Assert.NotNull(result);
                Assert.Equal(1, result.Count);
                Assert.True(result.First().ClientNames.Contains("[WD] Jussy"));
            }
        }

        private class LogEntries_Search : AbstractIndexCreationTask<LogEntry, LogEntries_Search.ReduceResult>
        {
            public LogEntries_Search()
            {
                Map = logEntries => from logEntry in logEntries
                                    where logEntry.ClientName != null
                                    select new
                                    {
                                        logEntry.ClientGuid,
                                        ClientNames = new[] { logEntry.ClientName },
                                        Query = new object[0]
                                    };

                Reduce = results => from result in results
                                    group result by result.ClientGuid
                                        into g
                                    select new
                                    {
                                        ClientGuid = g.Key,
                                        ClientNames = g.SelectMany(x => x.ClientNames),
                                        Query = new object[]
                                                           {
                                                                   g.Key,
                                                                   g.Key.ToString().Split('-', StringSplitOptions.None),
                                                                   g.SelectMany(x => x.ClientNames)
                                                           }
                                    };

                Indexes.Add(x => x.Query, FieldIndexing.Search);
                Store(x => x.Query, FieldStorage.No);
            }

            public class ReduceResult
            {
                public Guid ClientGuid { get; set; }
                public string[] ClientNames { get; set; }
                public string[] Query { get; set; }
            }
        }

        private class LogEntry
        {
            public string Id { get; set; }
            public Guid ClientGuid { get; set; }
            public string ClientName { get; set; }
        }

        private IList<LogEntries_Search.ReduceResult> Search(IDocumentStore documentStore, string query)
        {
            if (documentStore == null)
            {
                throw new ArgumentNullException("documentStore");
            }

            using (IDocumentSession documentSession = documentStore.OpenSession())
            {
                QueryStatistics stats;
                List<LogEntries_Search.ReduceResult> reduceResults = documentSession.Query<LogEntries_Search.ReduceResult, LogEntries_Search>()
                    .Statistics(out stats)
                    .Customize(x => x.WaitForNonStaleResults())
                    .Search(x => x.Query, query + "*")
                    .ToList();
                return reduceResults;
            }
        }

        private IDocumentStore CreateDocumentStore()
        {
            var documentStore = GetDocumentStore();

            // Wire up the index.
            new LogEntries_Search().Execute(documentStore);

            // Seed fake data.
            using (var documentSession = documentStore.OpenSession())
            {
                foreach (LogEntry logEntry in CreateFakeLogEntries())
                {
                    documentSession.Store(logEntry);
                }

                documentSession.SaveChanges();
            }

            return documentStore;
        }

        private static IEnumerable<LogEntry> CreateFakeLogEntries()
        {
            return new List<LogEntry>
            {
                new LogEntry
                    {
                        ClientGuid = new Guid("638867b0-82b9-11e1-b0c4-0800200c9a66"),
                        ClientName = "[WD] Pure Krome"
                    },
                new LogEntry
                    {
                        ClientGuid = new Guid("638867b0-82b9-11e1-b0c4-0800200c9a66"),
                        ClientName = "[WD] Jussy"
                    },
                new LogEntry
                    {
                        ClientGuid = new Guid("638867b0-82b9-11e1-b0c4-0800200c9a66"),
                        ClientName = "I'm a Hacker"
                    },
                new LogEntry
                    {
                        ClientGuid = new Guid("a053d7e8-ed82-464a-bfd4-37048b61947a"),
                        ClientName = "Some Dude"
                    },
                new LogEntry
                    {
                        ClientGuid = new Guid("1a03fd4d-94b7-4b34-88fc-f4037c0435ca"),
                        ClientName = "A_Lady"
                    },
                new LogEntry
                    {
                        ClientGuid = new Guid("74f594c7-b954-4dcb-93b6-624bbb3dfd15"),
                        ClientName = "KThxBai"
                    },
            };
        }
    }
}
