// -----------------------------------------------------------------------
//  <copyright file="VacancyCampaignsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class VacancyCampaignsTests : RavenTestBase
    {
        public class Vacancy
        {
            public string Id { get; set; }
            public string Category { get; set; }
            public List<Campaign> Campaigns { get; set; }

            public Vacancy()
            {
                Campaigns = new List<Campaign>();
            }
        }

        public class Campaign
        {
            public int Id { get; set; }
            public string Title { get; set; }
            public bool Active { get; set; }
        }

        public class VacancyCampaignsIndex : AbstractIndexCreationTask<Vacancy, VacancyCampaignsIndex.ReduceResult>
        {
            public VacancyCampaignsIndex()
            {
                Map = vacancies => from v in vacancies
                                   let c = v.Campaigns.LastOrDefault()
                                   where c != null
                                   select new
                                   {
                                       Id = v.Id,
                                       Category = v.Category,
                                       CampaignId = c.Id,
                                       Title = c.Title,
                                       Active = c.Active
                                   };

                Store(x => x.Id, FieldStorage.Yes);
                Store(x => x.CampaignId, FieldStorage.Yes);
            }


            public class ReduceResult
            {
                public string Id { get; set; }
                public string Category { get; set; }
                public int CampaignId { get; set; }
                public string Title { get; set; }
                public bool Active { get; set; }
            }
        }


        private static void CreateData(IDocumentStore store)
        {
            new VacancyCampaignsIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                var v1 = new Vacancy { Category = "Industrial" };
                v1.Campaigns.Add(new Campaign { Id = 1, Title = "Industrial Campaign 1", Active = false });
                v1.Campaigns.Add(new Campaign { Id = 2, Title = "Industrial Campaign 2", Active = true });
                session.Store(v1);

                var v2 = new Vacancy { Category = "Commercial" };
                v2.Campaigns.Add(new Campaign { Id = 1, Title = "Commercial Campaign 1", Active = true });
                session.Store(v2);

                session.SaveChanges();
            }
        }

        [Fact]
        public void Can_query_active_campaigns()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<VacancyCampaignsIndex.ReduceResult, VacancyCampaignsIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Active)
                        .ProjectFromIndexFieldsInto<VacancyCampaignsIndex.ReduceResult>()
                        .ToList();
                    Assert.Equal(2, results.Count());
                }
            }
        }
    }
}
