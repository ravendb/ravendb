using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Spatial
{
	public class PointObjectTests : RavenTest
	{
		public class SpatialDoc
		{
			public string Id { get; set; }
			public object Point { get; set; }
		}

		public class PointIndex : AbstractIndexCreationTask<SpatialDoc>
		{
			public PointIndex()
			{
				Map = docs => from doc in docs select new { doc.Point };

				Spatial(x => x.Point, x => x.Geography.Default());
			}
		}

		[Fact]
		public void PointTest()
		{
			using (var store = NewDocumentStore())
			{
				store.Initialize();
				store.ExecuteIndex(new PointIndex());

				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc { Point = null });
					session.Store(new SpatialDoc { Point = new [] { 45d, 45d } });
					session.Store(new SpatialDoc { Point = new { X = 45d, Y = 45d } });
					session.Store(new SpatialDoc { Point = new { Latitude = 45d, Longitude = 45d } });
					session.Store(new SpatialDoc { Point = new { lat = 45d, lon = 45d } });
					session.Store(new SpatialDoc { Point = new { lat = 45d, lng = 45d } });
				    session.Store(new SpatialDoc {Point = new {Lat = 45d, Long = 45d}});
                    session.Store(new SpatialDoc { Point = new [] { 45m, 45m } });
					session.Store(new SpatialDoc { Point = new { X = 45m, Y = 45m } });
					session.Store(new SpatialDoc { Point = new { Latitude = 45m, Longitude = 45m } });
					session.Store(new SpatialDoc { Point = new { lat = 45m, lon = 45m } });
					session.Store(new SpatialDoc { Point = new { lat = 45m, lng = 45m } });
					session.Store(new SpatialDoc { Point = new { Lat = 45m, Long = 45m } });
					session.Store(new SpatialDoc { Point = "geo:45.0,45.0,-78.4" });
					session.Store(new SpatialDoc { Point = "geo:45.0,45.0,-78.4;u=0.2" });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var matches = session.Query<SpatialDoc, PointIndex>()
										 .Spatial(x => x.Point, x => x.WithinRadiusOf(700, 40, 40))
					                     .Count();

					Assert.Equal(14, matches);
				}
			}
		}
	}
}