using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;


namespace Raven.Tests.Issues
{
   public class RavenDB_3302 : RavenTestBase
    {
        [Fact]
        public void CanIndexAndQuery()
        {
            DateTime now = DateTime.UtcNow;

            using (var store = NewDocumentStore())
            {
                new TicketTimerIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    foreach (var location in new[] {"locations-1", "locations-2"})
                    {
                        // Ticket updated right now ("Normal" timer status)
                        session.Store(new Ticket
                        {
                            LocationId = location,
                            DateUpdated = now,
                        });

                        // Ticket updated a year ago ("Critical" timer status)
                        session.Store(new Ticket
                        {
                            LocationId = location,
                            DateUpdated = now.AddYears(-1),
                        });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // Find all critical tickets for locations-1
                    var criticalTicketsForLocationQuery =
                        session.Advanced.DocumentQuery<TicketTimerIndex.IndexEntry, TicketTimerIndex>()
                        .WhereIn(entry => entry.LocationId, new[] { "locations-1" })
                        .AndAlso()
                        .Not.WhereEquals(entry => entry.CriticalDate, null)
                        .AndAlso()
                        .WhereLessThan(entry => entry.CriticalDate, now)
                        .SelectFields<TicketTimerIndex.IndexEntry>()
                        .WaitForNonStaleResults();

                    Console.WriteLine("{0} - Query: {1}", criticalTicketsForLocationQuery.ToList().Count, criticalTicketsForLocationQuery);
                    Assert.Equal(1, criticalTicketsForLocationQuery.ToList().Count);

                    // Find all critical tickets for locations-1 (same query as above, just wrapped in a subclause)
                    var criticalTicketsForLocationUsingSubclauseQuery =
                        session.Advanced.DocumentQuery<TicketTimerIndex.IndexEntry, TicketTimerIndex>()
                        .OpenSubclause()
                        .WhereIn(entry => entry.LocationId, new[] { "locations-1" })
                        .AndAlso()
                        .Not.WhereEquals(entry => entry.CriticalDate, null)
                        .AndAlso()
                        .WhereLessThan(entry => entry.CriticalDate, now)
                        .CloseSubclause()
                        .SelectFields<TicketTimerIndex.IndexEntry>()
                        .WaitForNonStaleResults();

                    Console.WriteLine("{0} - Query: {1}", criticalTicketsForLocationUsingSubclauseQuery.ToList().Count, criticalTicketsForLocationUsingSubclauseQuery);
                    Assert.Equal(1, criticalTicketsForLocationUsingSubclauseQuery.ToList().Count);
                }
            }
        }

        public class TicketTimerIndex : AbstractIndexCreationTask<Ticket>
        {
            public class IndexEntry
            {
                public string LocationId { get; set; }
                public DateTime DateUpdated { get; set; }
                public DateTime? CriticalDate { get; set; }
            }

            public TicketTimerIndex()
            {
                this.Map = tickets =>
                    from ticket in tickets
                    select new
                    {
                        ticket.LocationId,
                        ticket.DateUpdated,
                        // Tickets go into critical status after 3 months
                        CriticalDate = ticket.DateUpdated.AddMonths(3),
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        public class Ticket
        {
            public string LocationId { get; set; }
            public DateTime DateUpdated { get; set; }
        }
    }
}
