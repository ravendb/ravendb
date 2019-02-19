using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.MailingList.PhilJones
{
    public class PhilJones1 : RavenTestBase
    {
        private class Bar
        {
            public string AdminUserId { get; set; }
            public string Note { get; set; }
            public DateTime ReminderDue { get; set; }
        }

        private class Foo
        {
            public string Id { get; set; }
            public string AdminUserId { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }

            public List<Bar> Reminders { get; set; }
        }

        private class FooListViewModel
        {
            public string AdminUserId { get; set; }
            public string Note { get; set; }
            public DateTime ReminderDue { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string Id { get; set; }
        }

        private class Foos_BarProjection : AbstractIndexCreationTask<Foo>
        {
            public Foos_BarProjection()
            {
                Map = foos => from foo in foos
                              from bar in foo.Reminders
                              select new
                              {
                                  AdminUserId = bar.AdminUserId,
                                  Note = bar.Note,
                                  Reminders_ReminderDue = bar.ReminderDue,
                                  FirstName = foo.FirstName,
                                  LastName = foo.LastName,
                                  Id = foo.Id
                              };
            }
        }

        [Fact]
        public void ravendb_fails_to_map()
        {
            using (var store = GetDocumentStore())
            {
                new Foos_BarProjection().Execute(store);

                using (var session = store.OpenSession())
                {
                    var newReminder = new Foo
                    {
                        AdminUserId = "users/1",
                        FirstName = "Bob",
                        LastName = "Smith",
                        Reminders = new List<Bar>
                        {
                            new Bar { AdminUserId = "users/2", Note = "Ring Bob", ReminderDue = new DateTime(year: 2011, month: 12, day: 12)},
                            new Bar { AdminUserId = "users/3", Note ="Ring failed", ReminderDue = new DateTime(year:2011, month:12, day:11)}
                        }
                    };

                    session.Store(newReminder);
                    session.SaveChanges();

                    WaitForIndexing(store);

                    var reminders = session.Query<Foo, Foos_BarProjection>()
                        .Where(x => x.Reminders.Any(y => y.ReminderDue == new DateTime(2011, 12, 12)))
                        .ProjectInto<FooListViewModel>()
                        .ToList();

                    Assert.Equal(1, reminders.Count());
                }
            }
        }
    }
}
