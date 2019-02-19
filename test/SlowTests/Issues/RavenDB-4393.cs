using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4393 : RavenTestBase
    {
        private class FailedMessage
        {
            public string Id { get; set; }
        }

        private class ProcessedMessage
        {
            public string Id { get; set; }
            public TimeSpan CriticalTime { get; set; }
        }

        private class MessagesViewIndex : AbstractMultiMapIndexCreationTask<MessagesViewIndex.SortAndFilterOptions>
        {
            public class SortAndFilterOptions
            {
                public TimeSpan? CriticalTime { get; set; }
            }

            public MessagesViewIndex()
            {
                AddMap<ProcessedMessage>(messages => from message in messages
                                                     select new SortAndFilterOptions
                                                     {
                                                         CriticalTime = message.CriticalTime
                                                     });


                AddMap<FailedMessage>(messages => from message in messages
                                                  select new
                                                  {
                                                      CriticalTime = (TimeSpan?)null
                                                  });
            }
        }

        [Fact]
        public void SampleTestMethod()
        {
            using (var store = GetDocumentStore())
            {
                new MessagesViewIndex().Execute(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(new ProcessedMessage
                    {
                        Id = "1",
                        CriticalTime = TimeSpan.FromSeconds(10)
                    });

                    session.Store(new ProcessedMessage
                    {
                        Id = "2",
                        CriticalTime = TimeSpan.FromSeconds(20)
                    });

                    session.Store(new ProcessedMessage
                    {
                        Id = "3",
                        CriticalTime = TimeSpan.FromSeconds(15)
                    });

                    session.Store(new FailedMessage
                    {
                        Id = "4"
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var firstByCriticalTime = session.Query<MessagesViewIndex.SortAndFilterOptions, MessagesViewIndex>()
                        .Statistics(out stats)
                        .Where(x => x.CriticalTime != null)
                        .OrderBy(x => x.CriticalTime)
                        .ProjectInto<ProcessedMessage>()
                        .First();
                    Assert.Equal("1", firstByCriticalTime.Id);
                }
            }
        }
    }
}