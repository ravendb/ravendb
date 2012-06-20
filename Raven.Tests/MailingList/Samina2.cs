// -----------------------------------------------------------------------
//  <copyright file="Samina2.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Samina2 : RavenTest
	{
		public class PropertySearchingViewModel
		{
			public Guid Id { get; set; }
			public string UserFriendlyId { get; set; }
			public List<Unavailability> Unavailabilities { get; set; }
			public string UserFriendlyPropertyId { get; set; }

			public PropertySearchingViewModel()
			{
				Unavailabilities = new List<Unavailability>();
			}
		}

		public class Unavailability
		{
			public DateTime StartDay { get; set; }
			public DateTime EndDay { get; set; }
		}

		[Fact]
		public void Querying_a_sub_collection_in_an_index()
		{
			DateTime startDate = DateTime.Now;
			DateTime endDate = DateTime.Now.AddDays(10);
			
			using (var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					var model = new PropertySearchingViewModel() { Id = Guid.NewGuid(), UserFriendlyPropertyId = "p001" };
					model.Unavailabilities.Add(new Unavailability() { StartDay = startDate, EndDay = endDate });

					session.Store(model);
					session.SaveChanges();
				}

				new PropertiesSearchIndex().Execute(store);

				using(var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var count = session.Query<PropertySearchingViewModel, PropertiesSearchIndex>()
						.Statistics(out stats)
						.Customize(x=>x.WaitForNonStaleResults())
						.Count(x => x.Unavailabilities.Any(y => y.StartDay >= startDate && y.EndDay <= endDate));

					Assert.Equal(1, count);
					Assert.Equal("PropertiesSearchIndex", stats.IndexName);
				}
			}	
		}

		public class PropertiesSearchIndex : AbstractIndexCreationTask<PropertySearchingViewModel>
		{
			public PropertiesSearchIndex()
			{
				Map = items =>
				      from propertySearchingViewModel in items
				      from searchingViewModel in propertySearchingViewModel.Unavailabilities
				      select
				      	new
				      	{
				      		Unavailabilities_StartDay = searchingViewModel.StartDay,
				      		Unavailabilities_EndDay = searchingViewModel.EndDay
				      	};

			}
		}
	}
}