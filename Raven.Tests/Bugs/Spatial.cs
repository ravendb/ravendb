using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SpatialTest : RavenTest
	{
		public class MyDocumentItem
		{
			public DateTime Date { get; set; }
			public double? Latitude { get; set; }
			public double? Longitude { get; set; }
		}

		public class MyDocument
		{
			public string Id { get; set; }
			public MyDocumentItem[] Items { get; set; }
		}

		public class MyProjection
		{
			public string Id { get; set; }
			public DateTime Date { get; set; }
			public double Latitude { get; set; }
			public double Longitude { get; set; }
		}

		public class MyIndex : AbstractIndexCreationTask<MyDocument, MyProjection>
		{
			public MyIndex()
			{
				Map = docs =>
					from doc in docs
					from item in doc.Items
					let lat = item.Latitude ?? 0
					let lng = item.Longitude ?? 0
					select new
					{
						Id = doc.Id,
						Date = item.Date,

						Latitude = lat,
						Longitude = lng,
						_ = SpatialIndex.Generate(lat, lng)
					};

				Store(x => x.Id, FieldStorage.Yes);
				Store(x => x.Date, FieldStorage.Yes);

				Store(x => x.Latitude, FieldStorage.Yes);
				Store(x => x.Longitude, FieldStorage.Yes);
			}
		}

		[Fact]
		public void WeirdSpatialResults()
		{
			using (IDocumentStore store = NewDocumentStore())
			{
				using (IDocumentSession session = store.OpenSession())
				{
					session.Store(new MyDocument
					{
						Id = "First",
						Items = new[]
						{
							new MyDocumentItem
							{
								Date = new DateTime(2011, 1, 1),
								Latitude = 10,
								Longitude = 10
							}
						}
					});
					session.SaveChanges();

				}

				new MyIndex().Execute(store);
				using (IDocumentSession session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var result = session.Advanced
						.LuceneQuery<MyDocument, MyIndex>()
						.WaitForNonStaleResults()
						.WithinRadiusOf(0, 12.3456789f, 12.3456789f)
						.Statistics(out stats)
						.SelectFields<MyProjection>("Id", "Latitude", "Longitude")
						.Take(50)
						.ToArray();

					Assert.Equal(0, stats.TotalResults);
					Assert.Equal(0, result.Length); // Assert.AreEqual failed. Expected:<0>. Actual:<50>.
				}
			}
		}


		[Fact]
		public void MatchSpatialResults()
		{
			using (IDocumentStore store = NewDocumentStore())
			{
				using (IDocumentSession session = store.OpenSession())
				{
					session.Store(new MyDocument
					{
						Id = "First",
						Items = new[]
						{
							new MyDocumentItem
							{
								Date = new DateTime(2011, 1, 1),
								Latitude = 10,
								Longitude = 10
							}
						}
					});
					session.SaveChanges();

				}

				new MyIndex().Execute(store);
				using (IDocumentSession session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var result = session.Advanced
						.LuceneQuery<MyDocument, MyIndex>()
						.WaitForNonStaleResults()
						.WithinRadiusOf(1, 10, 10)
						.Statistics(out stats)
						.SelectFields<MyProjection>("Id", "Latitude", "Longitude")
						.Take(50)
						.ToArray();

					Assert.Equal(1, stats.TotalResults);
					Assert.Equal(1, result.Length); // Assert.AreEqual failed. Expected:<0>. Actual:<50>.
				}
			}
		}
	}
}