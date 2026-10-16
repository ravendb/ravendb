using System;
using System.Collections.Generic;
using System.Dynamic;
using MongoDB.Bson;
using Newtonsoft.Json;

namespace Raven.Migrator.MongoDB
{
    /// <summary>
    /// Converts BSON values to values that can be written as JSON:
    /// ObjectId, Decimal128, binary data (base64), regular expressions (/pattern/options), JavaScript code and symbols are written as strings,
    /// UUIDs as Guids (subtype 4 as standard, subtype 3 using the C# legacy byte order),
    /// dates as UTC DateTime and BSON types without a JSON equivalent (Timestamp, MinKey, MaxKey, JavaScript with scope, out of range dates)
    /// in the MongoDB relaxed extended JSON format, e.g. { "$timestamp": { "t": 1, "i": 1 } }.
    /// </summary>
    internal static class MongoBsonConverter
    {
        private static readonly global::MongoDB.Bson.IO.JsonWriterSettings RelaxedExtendedJsonSettings = new global::MongoDB.Bson.IO.JsonWriterSettings
        {
            OutputMode = global::MongoDB.Bson.IO.JsonOutputMode.RelaxedExtendedJson
        };

        public static ExpandoObject ToExpandoObject(BsonDocument bsonDocument)
        {
            var expandoObject = new ExpandoObject();
            var dictionary = (IDictionary<string, object>)expandoObject;

            foreach (var element in bsonDocument)
            {
                dictionary[element.Name] = ToDotNetValue(element.Value);
            }

            return expandoObject;
        }

        public static object ToDotNetValue(BsonValue value)
        {
            switch (value.BsonType)
            {
                case BsonType.Document:
                    return ToExpandoObject(value.AsBsonDocument);
                case BsonType.Array:
                    var array = value.AsBsonArray;
                    var list = new List<object>(array.Count);
                    foreach (var item in array)
                    {
                        list.Add(ToDotNetValue(item));
                    }
                    return list;
                case BsonType.String:
                    return value.AsString;
                case BsonType.Int32:
                    return value.AsInt32;
                case BsonType.Int64:
                    return value.AsInt64;
                case BsonType.Double:
                    return value.AsDouble;
                case BsonType.Decimal128:
                    return value.AsDecimal128.ToString();
                case BsonType.Boolean:
                    return value.AsBoolean;
                case BsonType.ObjectId:
                    return value.AsObjectId.ToString();
                case BsonType.Null:
                case BsonType.Undefined:
                    return null;
                case BsonType.DateTime:
                    var dateTime = value.AsBsonDateTime;
                    if (dateTime.IsValidDateTime)
                        return dateTime.ToUniversalTime();
                    return ToRelaxedExtendedJson(value);
                case BsonType.Binary:
                    var binaryData = value.AsBsonBinaryData;
                    if (TryGetGuid(binaryData, out var guid))
                        return guid;
                    return Convert.ToBase64String(binaryData.Bytes);
                case BsonType.RegularExpression:
                    var regex = value.AsBsonRegularExpression;
                    return $"/{regex.Pattern}/{regex.Options}";
                case BsonType.JavaScript:
                    return value.AsBsonJavaScript.Code;
                case BsonType.Symbol:
                    return value.AsBsonSymbol.Name;
                default:
                    // Timestamp, MinKey, MaxKey, JavaScriptWithScope
                    return ToRelaxedExtendedJson(value);
            }
        }

        public static string ToIdString(BsonValue value)
        {
            var dotNetValue = ToDotNetValue(value);
            if (dotNetValue is string str)
                return str;

            var json = JsonConvert.SerializeObject(dotNetValue);
            if (json.Length >= 2 && json[0] == '"' && json[^1] == '"')
                return JsonConvert.DeserializeObject<string>(json);

            return json;
        }

        public static bool TryGetGuid(BsonBinaryData binaryData, out Guid guid)
        {
            if (binaryData.Bytes.Length == 16)
            {
                switch (binaryData.SubType)
                {
                    case BsonBinarySubType.UuidStandard:
                        guid = GuidConverter.FromBytes(binaryData.Bytes, GuidRepresentation.Standard);
                        return true;
                    case BsonBinarySubType.UuidLegacy:
                        // the 2.x driver default representation
                        guid = GuidConverter.FromBytes(binaryData.Bytes, GuidRepresentation.CSharpLegacy);
                        return true;
                }
            }

            guid = default;
            return false;
        }

        private static object ToRelaxedExtendedJson(BsonValue value)
        {
            var json = new BsonDocument("value", value).ToJson(RelaxedExtendedJsonSettings);
            var dictionary = (IDictionary<string, object>)JsonConvert.DeserializeObject<ExpandoObject>(json, new JsonSerializerSettings { DateParseHandling = DateParseHandling.None });
            return dictionary["value"];
        }
    }
}
