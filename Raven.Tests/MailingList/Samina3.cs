// -----------------------------------------------------------------------
//  <copyright file="Samina3.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Samina3 : RavenTest
	{

		[Fact]
		public void Querying_a_sub_collection_in_an_index()
		{
			DateTime startDate1 = DateTime.Now;
			DateTime endDate1 = DateTime.Now.AddDays(10);
			DateTime startDate2 = DateTime.Now.AddDays(15);
			DateTime endDate2 = DateTime.Now.AddDays(20);

			using (GetNewServer())
			using (var store = new DocumentStore() { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var model = new SearchingViewModel() { Id = Guid.NewGuid(), UserFriendlyId = "p001", Feature = "Pool" };
					model.BookingRequests.Add(new BookingRequest() { StartDay = startDate1, EndDay = endDate1 });
					model.BookingRequests.Add(new BookingRequest() { StartDay = startDate2, EndDay = endDate2 });

					session.Store(model);
					session.SaveChanges();
				}

				new PropertiesSearchIndex().Execute(store);

				using (var session = store.OpenSession())
				{

					var facets = new List<Facet>
					{
						new Facet()
						{
							Name = "Feature"
						}
					};

					session.Store(new FacetSetup { Id = "facets/PropertySearchingFacets", Facets = facets });
					session.SaveChanges();

					RavenQueryStatistics stats;
					var query = session.Query<SearchingViewModel, PropertiesSearchIndex>()
						.Statistics(out stats)
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.BookingRequests.Any(y => y.StartDay >= startDate1 && y.EndDay <= endDate2));

					var result = query.ToList();

					var facetedCount = store.DatabaseCommands.GetFacets("PropertiesSearchIndex", new IndexQuery { Query = query.ToString() }, "facets/PropertySearchingFacets")["Feature"];


					Assert.Equal(1, result.Count());
					Assert.Equal(1, facetedCount.First(x => x.Range == "pool").Count);
					Assert.Equal("PropertiesSearchIndex", stats.IndexName);
				}
			}
		}
		public class SearchingViewModel
		{

			public Guid Id { get; set; }
			public string UserFriendlyId { get; set; }
			public string Feature { get; set; }

			public List<BookingRequest> BookingRequests { get; set; }

			public SearchingViewModel()
			{
				BookingRequests = new List<BookingRequest>();
			}
		}

		public class BookingRequest
		{
			public DateTime StartDay { get; set; }
			public DateTime EndDay { get; set; }
		}

		public class PropertiesSearchIndex : AbstractIndexCreationTask<SearchingViewModel>
		{
			public PropertiesSearchIndex()
			{
				Map = items =>
					  from searchingViewModel in items
					  select
						new
						{
							Feature = searchingViewModel.Feature,
							BookingRequests_StartDay = searchingViewModel.BookingRequests.Select(x => x.StartDay),
							BookingRequests_EndDay = searchingViewModel.BookingRequests.Select(x => x.EndDay)
						};
			}
		}
	}
}