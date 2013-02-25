using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class GeoJsonTests : RavenTest
	{
		public class SpatialDoc
		{
			public string Id { get; set; }
			public object Name { get; set; }
			public object GeoJson { get; set; }
		}

		public class GeoJsonIndex : AbstractIndexCreationTask<SpatialDoc>
		{
			public GeoJsonIndex()
			{
				Map = docs => from doc in docs select new { doc.Name, doc.GeoJson };

				Index(x => x.Name, FieldIndexing.Analyzed);
				Store(x => x.Name, FieldStorage.Yes);

				Spatial(x => x.GeoJson, x => x.Geography());
			}
		}

		[Fact]
		public void PointTest()
		{
			using (var store = new EmbeddableDocumentStore { RunInMemory = true })
			{
				store.Initialize();
				store.ExecuteIndex(new GeoJsonIndex());

				using (var session = store.OpenSession())
				{
					// @"{""type"":""Point"",""coordinates"":[45.0,45.0]}"
					session.Store(new SpatialDoc {
						                             Name = "dog",
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
					                     .Customize(x =>
					                     {
						                     x.WithinRadiusOf("GeoJson", 700, 40, 40);
						                     x.WaitForNonStaleResults();
					                     }).Any();

					Assert.True(matches);
				}
			}
		}
	}
}