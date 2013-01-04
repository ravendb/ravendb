using System;
using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class SelectManyIssue : RavenTest
	{
		[Fact]
		public void CanQueryProperly()
		{
			using (var store = NewDocumentStore())
			{
				new ClassificationSummary().Execute(store);

				using (var session = store.OpenSession())
				{
					for (var i = 0; i < 10; i++)
					{
						var request = new AdvertisementRequest
						{
							Id = Guid.NewGuid().ToString(),
							Impressions = new List<Ads>
							{
								new Ads
								{
									AdId = Guid.NewGuid().ToString(),
									ClassificationId = "9000",
									Click = new AdvertisementClick
									{
										ClickDate = SystemTime.UtcNow
									}

								},
								new Ads
								{
									AdId = Guid.NewGuid().ToString(),
									ClassificationId = "9000",
									Click = new AdvertisementClick
									{
										ClickDate = SystemTime.UtcNow
									}

								},
								new Ads
								{
									AdId = Guid.NewGuid().ToString(),
									ClassificationId = "9000"

								}
							}

						};

						session.Store(request);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var results = session.Query<dynamic, ClassificationSummary>()
						.Customize(x => x.WaitForNonStaleResults())
						.ToArray();

					WaitForUserToContinueTheTest(store);

					Assert.Equal(1, results.Length);
					Assert.Equal("9000", results[0].ClassificationId);
					Assert.Equal("30", results[0].Count);
					Assert.Equal("20", results[0].ClickCount);
				}
			}
		}

		public class ClassificationSummary : AbstractIndexCreationTask
		{
			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinition
				{
					Map = @"from request in docs.AdvertisementRequests
						from imp in request.Impressions
						select new {ClassificationId = imp.ClassificationId,Count = 1,ClickCount = imp.Click == null?0:1}",

					Reduce = @"from result in results group result by new {ClassificationId= result.ClassificationId} into g
						select new {ClassificationId = g.Key.ClassificationId,
						Count = g.Sum(x=>x.Count),ClickCount = g.Sum(x => x.ClickCount) }"
				};

			}
		}

		public class AdvertisementRequest
		{
			public string Id
			{
				get;
				set;
			}
			public List<Ads> Impressions { get; set; }
		}
		public class Ads
		{
			public string AdId { get; set; }
			public AdvertisementClick Click { get; set; }
			public string ClassificationId { get; set; }
		}
		public class AdvertisementClick
		{
			public DateTime ClickDate { get; set; }
		}

	}
}