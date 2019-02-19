// -----------------------------------------------------------------------
//  <copyright file="Bassler .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class Bassler : RavenTestBase
    {
        [Fact]
        public void Test()
        {
            using (var store = GetDocumentStore())
            {
                new App_WaiverWaitlistItemSearch().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new WaiverWaitlistItem { Id = "waiverwaitlistitems/1", ClientId = "clients/1", ScreeningDate = DateTime.Today, GroupNumber = "5" });
                    session.Store(new WaiverWaitlistItem { Id = "waiverwaitlistitems/2", ClientId = "clients/2", ScreeningDate = DateTime.Today.AddDays(7), GroupNumber = "1" });
                    session.Store(new TestClient { ClientProfile = { PersonName = new TestPersonName("John", "Able") }, Id = "clients/1" });
                    session.Store(new TestClient { ClientProfile = { PersonName = new TestPersonName("Joe", "Cain") }, Id = "clients/2" });
                    session.SaveChanges();

                    QueryStatistics stats;
                    var list = session.Query<App_WaiverWaitlistItemSearch.IndexResult, App_WaiverWaitlistItemSearch>()
                        .Statistics(out stats)
                        .Customize(x=>x.WaitForNonStaleResults())
                        .ProjectInto<App_WaiverWaitlistItemSearch.IndexResult>()
                        .ToList();

                    Assert.False(list.Any(result => result.GroupNumber == null));
                    Assert.False(list.Any(result => result.ScreeningDate == DateTime.MinValue));
                    Assert.False(list.Any(result => result.LastName == null));
                }
            }
        }

        private class WaiverWaitlistItem
        {
            public string Id { get; set; }
            public DateTime? ScreeningDate { get; set; }
            public string ClientId { get; set; }
            public string GroupNumber { get; set; }
        }

        private class TestClient
        {
            public TestClient()
            {
                ClientProfile = new ClientProfile();
            }

            public string Id { get; set; }
            public ClientProfile ClientProfile { get; set; }
        }

        private class ClientProfile
        {
            public TestPersonName PersonName { get; set; }
        }

        private class TestPersonName
        {
            public TestPersonName(string firstName, string lastName)
                : this(firstName, null, lastName, null)
            {
            }

            public TestPersonName(string firstName, string middleName, string lastName)
                : this(firstName, middleName, lastName, null)
            {
            }

            public TestPersonName(string firstName, string middleName, string lastName, string suffix)
            {
                FirstName = firstName;
                MiddleName = middleName;
                LastName = lastName;
                Suffix = suffix;
            }

            public string FirstName { get; private set; }
            public string MiddleName { get; private set; }
            public string LastName { get; private set; }
            public string Suffix { get; private set; }
        }

        private class App_WaiverWaitlistItemSearch : AbstractMultiMapIndexCreationTask<App_WaiverWaitlistItemSearch.IndexResult>
        {
            public class IndexResult
            {
                public string Id { get; set; }
                public DateTime? ScreeningDate { get; set; }
                public string ClientId { get; set; }
                public string LastName { get; set; }
                public string GroupNumber { get; set; }
            }

            public App_WaiverWaitlistItemSearch()
            {
                AddMap<WaiverWaitlistItem>(waiverWaitlistEntries => from waitlistEntry in waiverWaitlistEntries
                                                                    select new
                                                                    {
                                                                        Id = waitlistEntry.Id,
                                                                        ScreeningDate = waitlistEntry.ScreeningDate,
                                                                        ClientId = waitlistEntry.ClientId,
                                                                        LastName = (string)null,
                                                                        GroupNumber = waitlistEntry.GroupNumber,
                                                                    }
                             );

                AddMap<TestClient>(clients => from client in clients
                                              select new
                                              {
                                                  Id = client.Id,
                                                  ScreeningDate = DateTime.MinValue,
                                                  ClientId = client.Id,
                                                  LastName = client.ClientProfile.PersonName.LastName,
                                                  GroupNumber = (string)null,
                                              }
                               );



                Reduce = results =>
                    from result in results
                    group result by result.ClientId into g
                    let lastName = g.FirstOrDefault(x => x.LastName != null).LastName
                    let groupNumber = g.FirstOrDefault(x => x.GroupNumber != null).GroupNumber
                    let screeningDate = g.FirstOrDefault(x => x.ScreeningDate != DateTime.MinValue).ScreeningDate
                    from item in g
                    select new
                    {
                        Id = item.Id,
                        ScreeningDate = screeningDate,
                        ClientId = g.Key,
                        GroupNumber = groupNumber,
                        LastName = lastName,
                    };

                Store(ir => ir.ScreeningDate, FieldStorage.Yes);
                Store(ir => ir.ClientId, FieldStorage.Yes);
                Store(ir => ir.LastName, FieldStorage.Yes);
                Store(ir => ir.GroupNumber, FieldStorage.Yes);

                Indexes.Add(ir => ir.ScreeningDate, FieldIndexing.Search);
                Indexes.Add(ir => ir.ClientId, FieldIndexing.Search);
                Indexes.Add(ir => ir.LastName, FieldIndexing.Search);
                Indexes.Add(ir => ir.GroupNumber, FieldIndexing.Search);
            }
        }
    }
}
