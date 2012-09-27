// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList.RobStats
{
	public class StatisticsBug : RavenTest
	{
		[Fact]
		public void Should_get_stats_whe_using_lazy()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				new TheIndex().Execute(store);
				using (var session = store.OpenSession())
				{
					for (var i = 0; i < 15; i++)
					{
						var entity = new Entity
						{
							DisplayName = "Entity " + i,
							UpdatedAt = DateTimeOffset.Now,
							Visibility = "Visible"
						};

						session.Store(entity);

						var opinion = new Opinion
						{
							EntityId = entity.Id,
							IsFavorite = i%2 == 0
						};

						session.Store(opinion);
					}

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var query = session.Query<Summary, TheIndex>()
						.Customize(x => x.WaitForNonStaleResults())
						.Statistics(out stats)
						.Where(x => x.Visibility == "Visible")
						.OrderByDescending(x => x.UpdatedAt);

					var pagedQuery = query
						.Skip(0)
						.Take(10)
						.Lazily();


					var items = pagedQuery.Value.ToArray();
					Assert.Equal(15, stats.TotalResults);
					Assert.Equal(10, items.Length);
				}
			}
		}
	}
}