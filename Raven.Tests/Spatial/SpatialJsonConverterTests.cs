using System;
using System.Linq;
using GeoAPI.Geometries;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class SpatialJsonConverterTests : RavenTest
	{
		[Fact]
		public void Points()
		{
			using (var store = new EmbeddableDocumentStore { RunInMemory = true })
			{
				store.Conventions.CustomizeJsonSerializer = serializer => serializer.Converters.Add(new NtsRavenConverter());
				store.Initialize();
				store.ExecuteIndex(new CartesianIndex());

				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc {
						Geometry = new Polygon(new LinearRing(new[]
									{
										new Coordinate(1850, 1850), 
										new Coordinate(1950, 1850), 
										new Coordinate(1950, 1950), 
										new Coordinate(1850, 1950), 
										new Coordinate(1850, 1850), 
									}))
					});
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var matches = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new
						{
							type = "Point",
							coordinates = new[] { 1900, 1900 }
						}))
						.Any();

					Assert.True(matches);

					var matches1 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new Point(1900, 1900)))
						.Any();

					Assert.True(matches1);

					var matches2 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new[] { 1900, 1900 }))
						.Any();

					Assert.True(matches2);

					var matches3 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new { Latitude = 1900, Longitude = 1900 }))
						.Any();

					Assert.True(matches3);

					var matches4 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new { X = 1900, Y = 1900 }))
						.Any();

					Assert.True(matches4);

					var matches5 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new { lat = 1900, lng = 1900 }))
						.Any();

					Assert.True(matches5);

					var matches6 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new { lat = 1900, Long = 1900 }))
						.Any();

					Assert.True(matches6);

					var matches7 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new { lat = 1900, lon = 1900 }))
						.Any();

					Assert.True(matches7);
				}
			}
		}

		public class SpatialDoc
		{
			public string Id { get; set; }
			public IGeometry Geometry { get; set; }
		}

		public class CartesianIndex : AbstractIndexCreationTask<SpatialDoc>
		{
			public CartesianIndex()
			{
				Map = docs => from doc in docs select new { doc.Geometry };

				Spatial(x => x.Geometry, x => x.Cartesian(minX: 0, maxX: 2000, minY: 0, maxY: 2000, maxTreeLevel: 12));
			}
		}

		public class NtsRavenConverter : JsonConverter
		{
			private readonly WKTReader wktReader = new WKTReader();
			private readonly WKTWriter wktWriter = new WKTWriter(); 

			public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
			{
				writer.WriteValue(wktWriter.Write((IGeometry) value));
			}

			public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
			{
				if (reader.TokenType == JsonToken.Null)
					return null;

				return wktReader.Read((string)reader.Value);
			}

			public override bool CanConvert(Type objectType)
			{
				return typeof (IGeometry).IsAssignableFrom(objectType);
			}
		}
	}
}
