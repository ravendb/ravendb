using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using GeoAPI.Geometries;
using Lucene.Net.Util;
using NetTopologySuite.Geometries;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Spatial.JsonConverters.GeoJson;
using Xunit;
using System;
using Raven.Client.Document;
using System.Text;
using NetTopologySuite.Features;
using Constants = Raven.Abstractions.Data.Constants;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Tests.Spatial
{
	public class GeoJsonConverterTests : RavenTest
	{
		public IDocumentStore NewDocumentStore()
		{
			var store = new EmbeddableDocumentStore {RunInMemory = true};
			store.Conventions.CustomizeJsonSerializer = serializer =>
			{
				serializer.Converters.Add(new ICRSObjectConverter());
				serializer.Converters.Add(new FeatureCollectionConverter());
				serializer.Converters.Add(new FeatureConverter());
				serializer.Converters.Add(new AttributesTableConverter());
				serializer.Converters.Add(new GeometryConverter());
				serializer.Converters.Add(new GeometryArrayConverter());
				serializer.Converters.Add(new CoordinateConverter());
				serializer.Converters.Add(new EnvelopeConverter());
			};
			store.Initialize();
			store.ExecuteIndex(new CartesianIndex());
			return store;
		}

		[Fact]
		public void Point()
		{
			var point = new Point(50, 50);
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc { Geometry = point });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var doc = session.Query<SpatialDoc>().First();
					Assert.Equal(point, doc.Geometry);
				}

				using (var session = store.OpenSession())
				{
					var matches = session.Query<SpatialDoc, CartesianIndex>()
										  .Spatial(x => x.Geometry, x => x.WithinRadiusOf(20, 50, 50))
										  .Any();

					Assert.True(matches);
				}
			}
		}

		[Fact]
		public void LineString()
		{
			var lineString = new LineString(new[]
				                                {
					                                new Coordinate(1850, 1850),
					                                new Coordinate(1950, 1850),
					                                new Coordinate(1950, 1950),
					                                new Coordinate(1850, 1950),
				                                });

			using (var store = NewDocumentStore())
			{

				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc { Geometry = lineString });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var doc = session.Query<SpatialDoc>().First();
					Assert.Equal(lineString, doc.Geometry);
				}

				using (var session = store.OpenSession())
				{
					var lineString2 = new LineString(new[]
					                                {
						                                new Coordinate(1800, 1900),
						                                new Coordinate(1950, 2000),
					                                });

					var matches1 = session.Query<SpatialDoc, CartesianIndex>()
										  .Spatial(x => x.Geometry, x => x.Intersects(lineString2))
										  .Any();

					Assert.True(matches1);
				}
			}
		}

		[Fact]
		public void Polygon()
		{
			var polygon = new Polygon(new LinearRing(new[]
			                                         {
				                                         new Coordinate(1850, 1850),
				                                         new Coordinate(1950, 1850),
				                                         new Coordinate(1950, 1950),
				                                         new Coordinate(1850, 1950),
				                                         new Coordinate(1850, 1850),
			                                         }));

			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc { Geometry = polygon });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var doc = session.Query<SpatialDoc>().First();
					Assert.Equal(polygon, doc.Geometry);
				}

				using (var session = store.OpenSession())
				{
					var matches = session.Query<SpatialDoc, CartesianIndex>()
										  .Spatial(x => x.Geometry, x => x.Intersects(new Point(1900, 1900)))
										  .Any();

					Assert.True(matches);
				}
			}
		}

	    [Fact]
	    public void SpatialIndexWithoutException()
	    {
	        using (var documentStore = NewDocumentStore())
	        {
	            using (var bulkInsert = documentStore.BulkInsert())
                {
                    #region GeoJSON document definition
                    const string geoJsonDoc = @"
{
    ""Mrgid"": 4283,
    ""Shape"": {
        ""$type"": ""Geo.Geometries.Polygon, Geo"",
        ""Shell"": {
            ""Coordinates"": [
                [
                    -7.04858065671557,
                    58.026344760728
                ],
                [
                    -6.2065704622968,
                    58.3963839565032
                ],
                [
                    -5.19426698162231,
                    58.4948889984112
                ],
                [
                    -6.01683497405506,
                    56.9384804920236
                ],
                [
                    -6.02946985939349,
                    56.8879033623238
                ],
                [
                    -5.47937415735736,
                    55.7031051870622
                ],
                [
                    -5.42999621691786,
                    55.6523820743899
                ],
                [
                    -4.74858102193894,
                    55.4202683697592
                ],
                [
                    -4.9549279950283,
                    54.709145834366
                ],
                [
                    -5.42736295142527,
                    54.517888243511
                ],
                [
                    -6.06077268452169,
                    55.2904345145001
                ],
                [
                    -6.10235021108071,
                    55.3216124776687
                ],
                [
                    -8.05753322924901,
                    55.24977909031
                ],
                [
                    -7.44108597027722,
                    56.8877675212922
                ],
                [
                    -7.35835058866479,
                    56.9810822915857
                ],
                [
                    -7.33256521495613,
                    57.0336633858192
                ],
                [
                    -7.308601622737,
                    57.1606704245806
                ],
                [
                    -7.31160148386953,
                    57.387446113178
                ],
                [
                    -7.38614582933474,
                    57.5652045517982
                ],
                [
                    -7.04858065671557,
                    58.026344760728
                ]
            ],
            ""IsEmpty"": false,
            ""Is3D"": false,
            ""IsMeasured"": false,
            ""IsClosed"": true,
            ""__spatial"": ""LINEARRING (-7.04858065671557 58.026344760728, -6.2065704622968 58.3963839565032, -5.19426698162231 58.4948889984112, -6.01683497405506 56.9384804920236, -6.02946985939349 56.8879033623238, -5.47937415735736 55.7031051870622, -5.42999621691787 55.6523820743899, -4.74858102193894 55.4202683697592, -4.9549279950283 54.7091458343659, -5.42736295142527 54.517888243511, -6.06077268452169 55.2904345145001, -6.10235021108071 55.3216124776687, -8.05753322924901 55.24977909031, -7.44108597027722 56.8877675212922, -7.35835058866479 56.9810822915857, -7.33256521495613 57.0336633858192, -7.308601622737 57.1606704245807, -7.31160148386953 57.387446113178, -7.38614582933474 57.5652045517982, -7.04858065671557 58.026344760728)""
        },
        ""Holes"": [],
        ""IsEmpty"": false,
        ""Is3D"": false,
        ""IsMeasured"": false,
        ""__spatial"": ""POLYGON ((-7.04858065671557 58.026344760728, -6.2065704622968 58.3963839565032, -5.19426698162231 58.4948889984112, -6.01683497405506 56.9384804920236, -6.02946985939349 56.8879033623238, -5.47937415735736 55.7031051870622, -5.42999621691787 55.6523820743899, -4.74858102193894 55.4202683697592, -4.9549279950283 54.7091458343659, -5.42736295142527 54.517888243511, -6.06077268452169 55.2904345145001, -6.10235021108071 55.3216124776687, -8.05753322924901 55.24977909031, -7.44108597027722 56.8877675212922, -7.35835058866479 56.9810822915857, -7.33256521495613 57.0336633858192, -7.308601622737 57.1606704245807, -7.31160148386953 57.387446113178, -7.38614582933474 57.5652045517982, -7.04858065671557 58.026344760728))""
    }
}
";
                    #endregion

                    var metadata = new RavenJObject();
                    metadata.Add(Constants.RavenEntityName, "ZoneShapes");
                    bulkInsert.Store(RavenJObject.Parse(geoJsonDoc),metadata, "ZoneShapes/1");
	            }

                new ZoneShapesIndex().Execute(documentStore);
                WaitForIndexing(documentStore);

	            var stats = documentStore.DatabaseCommands.GetStatistics();
	            Assert.Empty(stats.Errors);	            
	        }
	    }
        [Fact]
        public void DefaultSpatialIndexWithoutException()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var bulkInsert = documentStore.BulkInsert())
                {
                    #region GeoJSON document definition
                    const string geoJsonDoc = @"
{
    ""Mrgid"": 4283,
    ""Shape"": {
        ""$type"": ""Geo.Geometries.Polygon, Geo"",
        ""Shell"": {
            ""Coordinates"": [
                [
                    -7.04858065671557,
                    58.026344760728
                ],
                [
                    -6.2065704622968,
                    58.3963839565032
                ],
                [
                    -5.19426698162231,
                    58.4948889984112
                ],
                [
                    -6.01683497405506,
                    56.9384804920236
                ],
                [
                    -6.02946985939349,
                    56.8879033623238
                ],
                [
                    -5.47937415735736,
                    55.7031051870622
                ],
                [
                    -5.42999621691786,
                    55.6523820743899
                ],
                [
                    -4.74858102193894,
                    55.4202683697592
                ],
                [
                    -4.9549279950283,
                    54.709145834366
                ],
                [
                    -5.42736295142527,
                    54.517888243511
                ],
                [
                    -6.06077268452169,
                    55.2904345145001
                ],
                [
                    -6.10235021108071,
                    55.3216124776687
                ],
                [
                    -8.05753322924901,
                    55.24977909031
                ],
                [
                    -7.44108597027722,
                    56.8877675212922
                ],
                [
                    -7.35835058866479,
                    56.9810822915857
                ],
                [
                    -7.33256521495613,
                    57.0336633858192
                ],
                [
                    -7.308601622737,
                    57.1606704245806
                ],
                [
                    -7.31160148386953,
                    57.387446113178
                ],
                [
                    -7.38614582933474,
                    57.5652045517982
                ],
                [
                    -7.04858065671557,
                    58.026344760728
                ]
            ],
            ""IsEmpty"": false,
            ""Is3D"": false,
            ""IsMeasured"": false,
            ""IsClosed"": true,
            ""__spatial"": ""LINEARRING (-7.04858065671557 58.026344760728, -6.2065704622968 58.3963839565032, -5.19426698162231 58.4948889984112, -6.01683497405506 56.9384804920236, -6.02946985939349 56.8879033623238, -5.47937415735736 55.7031051870622, -5.42999621691787 55.6523820743899, -4.74858102193894 55.4202683697592, -4.9549279950283 54.7091458343659, -5.42736295142527 54.517888243511, -6.06077268452169 55.2904345145001, -6.10235021108071 55.3216124776687, -8.05753322924901 55.24977909031, -7.44108597027722 56.8877675212922, -7.35835058866479 56.9810822915857, -7.33256521495613 57.0336633858192, -7.308601622737 57.1606704245807, -7.31160148386953 57.387446113178, -7.38614582933474 57.5652045517982, -7.04858065671557 58.026344760728)""
        },
        ""Holes"": [],
        ""IsEmpty"": false,
        ""Is3D"": false,
        ""IsMeasured"": false,
        ""__spatial"": ""POLYGON ((-7.04858065671557 58.026344760728, -6.2065704622968 58.3963839565032, -5.19426698162231 58.4948889984112, -6.01683497405506 56.9384804920236, -6.02946985939349 56.8879033623238, -5.47937415735736 55.7031051870622, -5.42999621691787 55.6523820743899, -4.74858102193894 55.4202683697592, -4.9549279950283 54.7091458343659, -5.42736295142527 54.517888243511, -6.06077268452169 55.2904345145001, -6.10235021108071 55.3216124776687, -8.05753322924901 55.24977909031, -7.44108597027722 56.8877675212922, -7.35835058866479 56.9810822915857, -7.33256521495613 57.0336633858192, -7.308601622737 57.1606704245807, -7.31160148386953 57.387446113178, -7.38614582933474 57.5652045517982, -7.04858065671557 58.026344760728))""
    }
}
";
                    #endregion

                    var metadata = new RavenJObject();
                    metadata.Add(Constants.RavenEntityName, "ZoneShapes");
                    bulkInsert.Store(RavenJObject.Parse(geoJsonDoc), metadata, "ZoneShapes/1");
                }

                new ZoneShapesDefaultIndex().Execute(documentStore);
                WaitForIndexing(documentStore);

                var stats = documentStore.DatabaseCommands.GetStatistics();
                Assert.Empty(stats.Errors);
            }

        }
        public class ZoneShape
		{
            public int Mrgid { get; set; }
            public IGeometry Shape { get; set; }
        }

        public class ZoneShapesDefaultIndex : AbstractIndexCreationTask<ZoneShape>
        {
            public ZoneShapesDefaultIndex()
            {
                Map = shapes => from shape in shapes
                                select new
                                {
                                    shape.Mrgid,
                                    shape.Shape
                                };

                Spatial(x => x.Shape, options => options.Geography.Default());
            }
        }
        public class ZoneShapesIndex : AbstractIndexCreationTask<ZoneShape>
        {
            public ZoneShapesIndex()
            {
                Map = shapes => from shape in shapes
                                select new
                                {
                                    shape.Mrgid,
                                    shape.Shape
                                };

                Spatial(x => x.Shape, options => options.Geography.GeohashPrefixTreeIndex(6));
            }
        }
	    public class
            SpatialDoc
		{
			public string Id { get; set; }
			public IGeometry Geometry { get; set; }
		}

		public class CartesianIndex : AbstractIndexCreationTask<SpatialDoc>
		{
			public CartesianIndex()
			{
				Map = docs => from doc in docs select new { doc.Geometry };

				Spatial(x => x.Geometry, x => x.Cartesian.QuadPrefixTreeIndex(12, new SpatialBounds(0, 0, 2000, 2000)));
			}
		}
	}
}