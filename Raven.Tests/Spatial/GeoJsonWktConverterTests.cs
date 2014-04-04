using System;
using NetTopologySuite.IO;
using Raven.Abstractions.Spatial;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Spatial
{
	public class GeoJsonWktConverterTests : NoDisposalNeeded
	{
		private readonly GeoJsonWktConverter reader;
		private readonly WKTReader wktReader = new WKTReader();

		public GeoJsonWktConverterTests()
		{
			reader = new GeoJsonWktConverter();
		}

		private void AssertEqual(string json, string wkt)
		{
			string obj;
			reader.TryConvert(RavenJObject.Parse(json), out obj);
			Console.WriteLine(obj);
			Assert.Equal(wktReader.Read(obj), wktReader.Read(wkt));
		}

		[Fact]
		public void Point()
		{
			AssertEqual(
				@"{""type"":""Point"",""coordinates"":[0,0]}",
				@"POINT (0 0)"
				);
		}

		[Fact]
		public void LineString()
		{
			AssertEqual(
				@"{""type"":""LineString"",""coordinates"":[[0,0],[1,1]]}",
				@"LINESTRING (0 0, 1 1)"
				);
		}

		[Fact]
		public void Polygon()
		{
			AssertEqual(
				@"{""type"":""Polygon"",""coordinates"":[[[0,0],[1,1],[2,0],[0,0]]]}",
				@"POLYGON ((0 0, 1 1, 2 0, 0 0))"
				);
		}

		[Fact]
		public void MultiPoint()
		{
			AssertEqual(
				@"{""type"":""MultiPoint"",""coordinates"":[[0,0],[1,1],[2,0],[0,0]]}",
				@"MULTIPOINT (0 0, 1 1, 2 0, 0 0)"
				);
		}

		[Fact]
		public void MultiLineString()
		{
			AssertEqual(
				@"{""type"":""MultiLineString"",""coordinates"":[[[0,0],[1,1]]]}",
				@"MULTILINESTRING ((0 0, 1 1))"
				);
		}

		[Fact]
		public void MultiPolygon()
		{
			AssertEqual(
				@"{""type"":""MultiPolygon"",""coordinates"":[[[[0,0],[1,1],[2,0],[0,0]]]]}",
				@"MULTIPOLYGON (((0 0, 1 1, 2 0, 0 0)))"
				);
		}

		//[Fact]
		//public void GeometryCollection()
		//{
		//	AssertEqual(
		//		@"{""type"":""GeometryCollection"",""geometries"":[{""type"":""Point"",""coordinates"":[0,0]},{""type"":""Point"",""coordinates"":[0,1]}]}",
		//		@"GEOMETRYCOLLECTION(POINT (0 0), POINT (0 1))"
		//		);
		//}
	}
}