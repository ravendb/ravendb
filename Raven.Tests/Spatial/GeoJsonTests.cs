using System.Collections.Generic;
using System.Linq;
using GeoAPI.Geometries;
using NetTopologySuite.Geometries;
using Raven.Database.Indexing.Spatial.GeoJson;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Spatial
{
    public class GeoJsonTests
    {
	    private readonly IGeometryFactory geometryFactory;
	    private readonly GeoJsonReader reader;

		public GeoJsonTests()
		{
			geometryFactory = GeometryFactory.Default;
			reader = new GeoJsonReader(geometryFactory);
		}

		private void AssertEqual(string json, object geometry)
		{
			object obj;
			reader.TryRead(RavenJObject.Parse(json), out obj);
			Assert.Equal(obj, geometry);
		}

        [Fact]
        public void Point()
        {
			AssertEqual(
				@"{""type"":""Point"",""coordinates"":[0,0]}",
				geometryFactory.CreatePoint(new Coordinate(0, 0))
			);
        }

        [Fact]
        public void LineString()
		{
			AssertEqual(
				@"{""type"":""LineString"",""coordinates"":[[0,0],[1,1]]}",
				geometryFactory.CreateLineString(new[] { new Coordinate(0, 0), new Coordinate(1, 1) })
			);
        }

        [Fact]
        public void Polygon()
		{
			AssertEqual(
				@"{""type"":""Polygon"",""coordinates"":[[[0,0],[1,1],[2,0],[0,0]]]}",
				geometryFactory.CreatePolygon(new[] { new Coordinate(0, 0), new Coordinate(1, 1), new Coordinate(2, 0), new Coordinate(0, 0) })
			);
        }

        [Fact]
        public void MultiPoint()
		{
			AssertEqual(
				@"{""type"":""MultiPoint"",""coordinates"":[[0,0],[1,1],[2,0],[0,0]]}",
				geometryFactory.CreateMultiPoint(new[] { new Coordinate(0, 0), new Coordinate(1, 1), new Coordinate(2, 0), new Coordinate(0, 0) })
			);
        }

        [Fact]
        public void MultiLineString()
		{
			AssertEqual(
				@"{""type"":""MultiLineString"",""coordinates"":[[[0,0],[1,1]]]}",
				geometryFactory.CreateMultiLineString(new[] { geometryFactory.CreateLineString(new[] { new Coordinate(0, 0), new Coordinate(1, 1) }) })
			);
        }

        [Fact]
        public void MultiPolygon()
		{
			AssertEqual(
				@"{""type"":""MultiPolygon"",""coordinates"":[[[[0,0],[1,1],[2,0],[0,0]]]]}",
				geometryFactory.CreateMultiPolygon(new[] { geometryFactory.CreatePolygon(new[] {new Coordinate(0, 0), new Coordinate(1, 1), new Coordinate(2, 0), new Coordinate(0, 0)})})
			);
        }

        [Fact]
        public void GeometryCollection()
		{
			AssertEqual(
				@"{""type"":""GeometryCollection"",""geometries"":[{""type"":""Point"",""coordinates"":[0,0]},{""type"":""Point"",""coordinates"":[0,1]}]}",
				geometryFactory.CreateGeometryCollection(new IGeometry[] { geometryFactory.CreatePoint(new Coordinate(0, 0)), geometryFactory.CreatePoint(new Coordinate(0, 1)) })
			);
        }
    }
}
