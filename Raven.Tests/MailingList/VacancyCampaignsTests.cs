// -----------------------------------------------------------------------
//  <copyright file="VacancyCampaignsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.MailingList
{
	public class VacancyCampaignsTests : RavenTest
	{
		private EmbeddableDocumentStore Store;

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

				TransformResults = (store, results) => from result in results
													   let vacancy = store.Load<Vacancy>(result.Id)
													   let campaign = vacancy.Campaigns.FirstOrDefault(c => c.Id.ToString() == result.CampaignId.ToString())
													   select new
													   {
														   Id = result.Id,
														   Category = vacancy.Category,
														   CampaignId = result.CampaignId,
														   Title = campaign.Title,
														   Active = campaign.Active
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

		public VacancyCampaignsTests()
		{
			Store = NewDocumentStore();
			new VacancyCampaignsIndex().Execute(Store);
			using (var session = Store.OpenSession())
			{
				var v1 = new Vacancy { Category = "Industrial" };
				v1.Campaigns.Add(new Campaign { Id = 1, Title = "Industrial Campaign 1", Active = false });
				v1.Campaigns.Add(new Campaign { Id = 2, Title = "Industrial Campaign 2", Active = true });
				session.Store(v1);

				var v2 = new Vacancy { Category = "Commerical" };
				v2.Campaigns.Add(new Campaign { Id = 1, Title = "Commercial Campaign 1", Active = true });
				session.Store(v2);

				session.SaveChanges();
			}
		}

		public override void Dispose()
		{
			Store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void Can_query_active_campaigns()
		{
			using (var session = Store.OpenSession())
			{
				var results = session.Query<VacancyCampaignsIndex.ReduceResult, VacancyCampaignsIndex>()
					.Customize(x => x.WaitForNonStaleResults())
					.Where(x => x.Active)
					.AsProjection<VacancyCampaignsIndex.ReduceResult>()
					.ToList();
				Assert.Equal(2, results.Count());
			}

		}
	}
}