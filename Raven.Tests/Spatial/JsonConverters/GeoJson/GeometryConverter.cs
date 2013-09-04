using Raven.Imports.Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using GeoAPI.Geometries;
using NetTopologySuite.Geometries;
using Raven.Imports.Newtonsoft.Json.Linq;

// From: https://code.google.com/p/nettopologysuite/source/browse/#svn%2Ftrunk%2FNetTopologySuite.IO%2FNetTopologySuite.IO.GeoJSON
namespace Raven.Tests.Spatial.JsonConverters.GeoJson
{
	public class GeometryConverter : JsonConverter
    {
        private readonly IGeometryFactory _factory;
        
        public GeometryConverter()
            :this(GeometryFactory.Default)
        {
            
        }

        public GeometryConverter(IGeometryFactory geometryFactory)
        {
            this._factory = geometryFactory;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            IGeometry geom = value as IGeometry;
            if (geom == null)
                return;

            writer.WriteStartObject();

            GeoJsonObjectType geomType = this.ToGeoJsonObject(geom);
            writer.WritePropertyName("type");
            writer.WriteValue(Enum.GetName(typeof(GeoJsonObjectType), geomType));
            
            switch (geomType)
            {
                case GeoJsonObjectType.Point:
                    serializer.Serialize(writer, geom.Coordinate);
                    break;
                case GeoJsonObjectType.LineString:
                case GeoJsonObjectType.MultiPoint:
                    serializer.Serialize(writer, geom.Coordinates);
                    break;
                case GeoJsonObjectType.Polygon:
                    IPolygon poly = geom as IPolygon;
                    Debug.Assert(poly != null);
                    serializer.Serialize(writer, PolygonCoordiantes(poly));
                    break;

                case GeoJsonObjectType.MultiPolygon:
                    IMultiPolygon mpoly = geom as IMultiPolygon;
                    Debug.Assert(mpoly != null);
                    List<List<Coordinate[]>> list = new List<List<Coordinate[]>>();
                    foreach (IPolygon mempoly in mpoly.Geometries)
                        list.Add(PolygonCoordiantes(mempoly));
                    serializer.Serialize(writer, list);
                    break;

                case GeoJsonObjectType.GeometryCollection:
                    IGeometryCollection gc = geom as IGeometryCollection;
                    Debug.Assert(gc != null);
                    serializer.Serialize(writer, gc.Geometries);
                    break;
                default:
                    List<Coordinate[]> coordinates = new List<Coordinate[]>();
                    foreach (IGeometry geometry in ((IGeometryCollection)geom).Geometries)
                        coordinates.Add(geometry.Coordinates);
                    serializer.Serialize(writer, coordinates);
                    break;
            }

            writer.WriteEndObject();
        }



        private GeoJsonObjectType ToGeoJsonObject(IGeometry geom)
        {
            if (geom is IPoint)
                return GeoJsonObjectType.Point;
            if (geom is ILineString)
                return GeoJsonObjectType.LineString;
            if (geom is IPolygon)
                return GeoJsonObjectType.Polygon;
            if (geom is IMultiPoint)
                return GeoJsonObjectType.MultiPoint;
            if (geom is IMultiLineString)
                return GeoJsonObjectType.MultiLineString;
            if (geom is IMultiPolygon)
                return GeoJsonObjectType.MultiPolygon;
            if (geom is IGeometryCollection)
                return GeoJsonObjectType.GeometryCollection;

            throw new ArgumentException("geom");
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            reader.Read();
            if (!(reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "type"))
            {
                throw new Exception();
            }
            reader.Read();
            if (reader.TokenType != JsonToken.String)
            {
                throw new Exception();
            }

            GeoJsonObjectType geometryType = (GeoJsonObjectType)Enum.Parse(typeof (GeoJsonObjectType), (string) reader.Value);
            switch (geometryType)
            {
                case GeoJsonObjectType.Point:
                    Coordinate coordinate = serializer.Deserialize<Coordinate>(reader);
                    return this._factory.CreatePoint(coordinate);
                
                case GeoJsonObjectType.LineString:
                    Coordinate[] coordinates = serializer.Deserialize<Coordinate[]>(reader);
                    return this._factory.CreateLineString(coordinates);
                
                case GeoJsonObjectType.Polygon:
                    List<Coordinate[]> coordinatess = serializer.Deserialize<List<Coordinate[]>>(reader);
                    return this.CreatePolygon(coordinatess);

                case GeoJsonObjectType.MultiPoint:
                    coordinates = serializer.Deserialize<Coordinate[]>(reader);
                    return this._factory.CreateMultiPoint(coordinates);

                case GeoJsonObjectType.MultiLineString:
                    coordinatess = serializer.Deserialize<List<Coordinate[]>>(reader);
                    List<ILineString> strings = new List<ILineString>();
                    for (int i = 0; i < coordinatess.Count; i++)
                        strings.Add(this._factory.CreateLineString(coordinatess[i]));
                    return this._factory.CreateMultiLineString(strings.ToArray());
                
                case GeoJsonObjectType.MultiPolygon:
                    List<List<Coordinate[]>> coordinatesss = serializer.Deserialize<List<List<Coordinate[]>>>(reader);
                    List<IPolygon> polygons = new List<IPolygon>();
                    foreach (List<Coordinate[]> coordinateses in coordinatesss)
                        polygons.Add(this.CreatePolygon(coordinateses));
                    return this._factory.CreateMultiPolygon(polygons.ToArray());

                case GeoJsonObjectType.GeometryCollection:
                    List<IGeometry> geoms = serializer.Deserialize<List<IGeometry>>(reader);
                    return this._factory.CreateGeometryCollection(geoms.ToArray());
                    //ReadJson(reader,)
            }

            return null;
        }

        private static List<Coordinate[]> PolygonCoordiantes(IPolygon polygon)
        {
            List<Coordinate[]> res = new List<Coordinate[]>();
            res.Add(polygon.Shell.Coordinates);
            foreach (ILineString interiorRing in polygon.InteriorRings)
                res.Add(interiorRing.Coordinates);
            return res;
        }

        private IPolygon CreatePolygon(IList<Coordinate[]> coordinatess)
        {
            ILinearRing shell = this._factory.CreateLinearRing(coordinatess[0]);
            List<ILinearRing> rings = new List<ILinearRing>();
            for (int i = 1; i < coordinatess.Count; i++)
                rings.Add(this._factory.CreateLinearRing(coordinatess[i]));
            return this._factory.CreatePolygon(shell, rings.ToArray());
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(IGeometry).IsAssignableFrom(objectType); // && !objectType.IsAbstract;
        }
    }

    public class GeometryArrayConverter : JsonConverter
    {
        private readonly IGeometryFactory _factory;

        public GeometryArrayConverter()
            :this(GeometryFactory.Default)
        {
        }
        public GeometryArrayConverter(IGeometryFactory factory)
        {
            this._factory = factory;
        }
        
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WritePropertyName("geometries");
            WriteGeometries(writer, value as IList<IGeometry>, serializer);
        }

        private static void WriteGeometries(JsonWriter writer, IEnumerable<IGeometry> geometries, JsonSerializer serializer)
        {
            writer.WriteStartArray();
            foreach (IGeometry geometry in geometries)
                serializer.Serialize(writer, geometry);
            writer.WriteEndArray();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            reader.Read();
            if (!(reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "geometries"))
            {
                throw new Exception();
            }
            reader.Read();
            if (reader.TokenType != JsonToken.StartArray)
                throw new Exception();

            reader.Read();
            List<IGeometry> geoms = new List<IGeometry>();
            while (reader.TokenType != JsonToken.EndArray)
            {
                JObject obj = (JObject)serializer.Deserialize(reader);
                GeoJsonObjectType geometryType = (GeoJsonObjectType)Enum.Parse(typeof(GeoJsonObjectType), obj.Value<string>("type"));

                switch (geometryType)
                {
                    case GeoJsonObjectType.Point:
                        geoms.Add(this._factory.CreatePoint(ToCoordinate(obj.Value<JArray>("coordinates"))));
                        break;
                    case GeoJsonObjectType.LineString:
                        geoms.Add(this._factory.CreateLineString(ToCoordinates(obj.Value<JArray>("coordinates"))));
                        break;
                    case GeoJsonObjectType.Polygon:
                        geoms.Add(this.CreatePolygon(ToListOfCoordinates(obj.Value<JArray>("coordinates"))));
                        break;
                    case GeoJsonObjectType.MultiPoint:
                        geoms.Add(this._factory.CreateMultiPoint(ToCoordinates(obj.Value<JArray>("coordinates"))));
                        break;
                    case GeoJsonObjectType.MultiLineString:
                        geoms.Add(this.CreateMultiLineString(ToListOfCoordinates(obj.Value<JArray>("coordinates"))));
                        break;
                    case GeoJsonObjectType.MultiPolygon:
                        geoms.Add(this.CreateMultiPolygon(ToListOfListOfCoordinates(obj.Value<JArray>("coordinates"))));
                        break;
                    case GeoJsonObjectType.GeometryCollection:
                        throw new NotSupportedException();

                }
                reader.Read();
            }
            return geoms;
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(IEnumerable<IGeometry>).IsAssignableFrom(objectType);
        }

        private IMultiLineString CreateMultiLineString(List<Coordinate[]> coordinates)
        {
            ILineString[] strings = new ILineString[coordinates.Count];
            for (int i = 0; i < coordinates.Count; i++)
                strings[i] = this._factory.CreateLineString(coordinates[i]);
            return this._factory.CreateMultiLineString(strings);
        }

        private IPolygon CreatePolygon(List<Coordinate[]> coordinates)
        {
            ILinearRing shell = this._factory.CreateLinearRing(coordinates[0]);
            ILinearRing[] rings = new ILinearRing[coordinates.Count - 1];
            for (int i = 1; i < coordinates.Count; i++)
                rings[i - 1] = this._factory.CreateLinearRing(coordinates[i]);
            return this._factory.CreatePolygon(shell, rings);
        }

        private IMultiPolygon CreateMultiPolygon(List<List<Coordinate[]>> coordinates)
        {
            IPolygon[] polygons = new IPolygon[coordinates.Count];
            for (int i = 0; i < coordinates.Count; i++)
                polygons[i] = this.CreatePolygon(coordinates[i]);
            return this._factory.CreateMultiPolygon(polygons);
        }
        
        private static Coordinate ToCoordinate(JArray array)
            {
                Coordinate c = new Coordinate {X = (Double) array[0], Y = (Double) array[1]};
                if (array.Count > 2)
                    c.Z = (Double)array[2];
                return c;
            }

            public static Coordinate[] ToCoordinates(JArray array)
            {
                Coordinate[] c = new Coordinate[array.Count];
                for (int i = 0; i < array.Count; i++)
                {
                    c[i] = ToCoordinate((JArray) array[i]);
                }
                return c;
            }
            public static List<Coordinate[]> ToListOfCoordinates(JArray array)
            {
                List<Coordinate[]> c = new List<Coordinate[]>();
                for (int i = 0; i < array.Count; i++)
                    c.Add(ToCoordinates((JArray)array[i]));
                return c;
            }
            public static List<List<Coordinate[]>> ToListOfListOfCoordinates(JArray array)
            {
                List<List<Coordinate[]>> c = new List<List<Coordinate[]>>();
                for (int i = 0; i < array.Count; i++)
                    c.Add(ToListOfCoordinates((JArray)array[i]));
                return c;
            }
    }
}