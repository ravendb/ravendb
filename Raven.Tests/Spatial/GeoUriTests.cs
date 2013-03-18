using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class GeoUriTests : RavenTest
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

				Spatial(x => x.Point, x => x.Geography());
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
					session.Store(new SpatialDoc { Point = "geo:45.0,45.0,-78.4" });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var matches = session.Query<SpatialDoc, PointIndex>()
										 .Spatial(x => x.Point, x => x.Within("geo:45.0,45.0,-78.4;u=1.0"))
										 .Any();

					Assert.True(matches);
				}
			}
		}
	}
}
