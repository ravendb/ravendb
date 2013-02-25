using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class PointObjectTests : RavenTest
	{
		public class SpatialDoc
		{
			public string Id { get; set; }
			public object Name { get; set; }
			public object Point { get; set; }
		}

		public class PointIndex : AbstractIndexCreationTask<SpatialDoc>
		{
			public PointIndex()
			{
				Map = docs => from doc in docs select new { doc.Point };

				Spatial(x => x.Point, x => x.Geography());
			}
		}

		[Fact]
		public void PointTest()
		{
			using (var store = new EmbeddableDocumentStore { RunInMemory = true })
			{
				store.Initialize();
				store.ExecuteIndex(new PointIndex());

				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc { Point = new { X = 45d, Y = 45d } });
					session.Store(new SpatialDoc { Point = new { Latitude = 45d, Longitude = 45d } });
					session.Store(new SpatialDoc { Point = new { lat = 45d, lon = 45d } });
					session.Store(new SpatialDoc { Point = new { lat = 45d, lng = 45d } });
					session.Store(new SpatialDoc { Point = new { Lat = 45d, Long = 45d } });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var matches = session.Query<SpatialDoc, PointIndex>()
					                     .Customize(x =>
					                     {
						                     x.WithinRadiusOf("Point", 700, 40, 40);
						                     x.WaitForNonStaleResults();
					                     }).Count();

					Assert.Equal(5, matches);
				}
			}
		}
	}
}