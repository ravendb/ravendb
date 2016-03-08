using System;
using System.Linq;

using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Xunit;

namespace Testing
{

    public class RavenDB_4393 : RavenTest
    {
        public class FailedMessage
        {
            public string Id { get; set; }
        }

        public class ProcessedMessage
        {
            public string Id { get; set; }
            public TimeSpan CriticalTime { get; set; }
        }

        public class MessagesViewIndex : AbstractMultiMapIndexCreationTask<MessagesViewIndex.SortAndFilterOptions>
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
            using (IDocumentStore store = NewDocumentStore())
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
                    RavenQueryStatistics stats;
                    var firstByCriticalTime = session.Query<MessagesViewIndex.SortAndFilterOptions, MessagesViewIndex>()
                        .Statistics(out stats)                        
                        .Where(x => x.CriticalTime != null)
                        .OrderBy(x => x.CriticalTime)
                        .ProjectFromIndexFieldsInto<ProcessedMessage>()
                        .First();
                    Assert.Equal("1", firstByCriticalTime.Id);
                }
            }
        }
    }
}