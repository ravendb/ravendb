using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Spatial
{
	public class GeoJsonTests : RavenTest
	{
		public class SpatialDoc
		{
			public string Id { get; set; }
			public object GeoJson { get; set; }
		}

		public class GeoJsonIndex : AbstractIndexCreationTask<SpatialDoc>
		{
			public GeoJsonIndex()
			{
				Map = docs => from doc in docs select new { doc.GeoJson };

				Spatial(x => x.GeoJson, x => x.Geography.Default());
			}
		}

		[Fact]
		public void PointTest()
		{
			using (var store = NewDocumentStore())
			{
				store.Initialize();
				store.ExecuteIndex(new GeoJsonIndex());

				using (var session = store.OpenSession())
				{
					// @"{""type"":""Point"",""coordinates"":[45.0,45.0]}"
					session.Store(new SpatialDoc {
						                             GeoJson = new {
							                                           type = "Point",
							                                           coordinates = new[] { 45d, 45d }
						                                           } });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var matches = session.Query<SpatialDoc, GeoJsonIndex>()
					                     .Customize(x => x.WithinRadiusOf("GeoJson", 700, 40, 40))
										 .Any();

					Assert.True(matches);
				}
			}
		}
	}
}