using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using  Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Util
{
    public static class DocumentHelpers
    {
        /// <summary>
        /// gets rough size of RavenJToken - in bytes
        /// </summary>
        public  static long GetRoughSize(RavenJToken token)
        {
            long sum;
            switch (token.Type)
            {
                case JTokenType.None:
                    return 0;
                case JTokenType.Object:
                    sum = 2;// {}
                    foreach (var prop in (RavenJObject)token)
                    {
                        sum += prop.Key.Length + 1; // name:
                        sum += GetRoughSize(prop.Value);
                    }
                    return sum;
                case JTokenType.Array:
                    // the 1 is for ,
                    return 2 + ((RavenJArray)token).Sum(prop => 1 + GetRoughSize(prop));
                case JTokenType.Constructor:
                case JTokenType.Property:
                case JTokenType.Comment:
                case JTokenType.Raw:
                    return 0;
                case JTokenType.Boolean:
                    return token.Value<bool>() ? 4 : 5;
                case JTokenType.Null:
                    return 4;
                case JTokenType.Undefined:
                    return 9;
                case JTokenType.Date:
                    return 21;
                case JTokenType.Bytes:
                case JTokenType.Integer:
                case JTokenType.Float:
                case JTokenType.String:
                case JTokenType.Guid:
                case JTokenType.TimeSpan:
                case JTokenType.Uri:
                    return token.Value<string>().Length;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public static IEnumerable<string> GetPropertiesFromDocuments(IEnumerable<JsonDocument> jsonDocuments, bool includeNestedProperties)
        {
            return
                jsonDocuments.SelectMany(doc =>
                    GetPropertiesFromJObject(doc.DataAsJson, parentPropertyPath: "",
                    includeNestedProperties: includeNestedProperties));
        }

        public static IEnumerable<string> GetMetadataFromDocuments(IEnumerable<JsonDocument> jsonDocuments, bool includeNestedProperties)
        {
            return
                jsonDocuments.SelectMany(doc =>
                    GetPropertiesFromJObject(doc.Metadata, parentPropertyPath: "",
                    includeNestedProperties: includeNestedProperties));
        }

        public static IEnumerable<string> GetPropertiesFromJObjects(IEnumerable<RavenJObject> jObjects, bool includeNestedProperties, bool includeMetadata = true, bool excludeParentPropertyNames = false)
        {
            return jObjects.SelectMany(doc => GetPropertiesFromJObject(doc, "", includeNestedProperties, includeMetadata, excludeParentPropertyNames));
        }

        public static IEnumerable<string> GetPropertiesFromJObject(RavenJObject jObject, string parentPropertyPath, bool includeNestedProperties, bool includeMetadata = true, bool excludeParentPropertyNames = false)
        {
            var properties = from property in jObject
                             select
                                 new
                                 {
                                     Path = parentPropertyPath + (String.IsNullOrEmpty(parentPropertyPath) ? "" : ".") + property.Key,
                                     property.Value
                                 };

            foreach (var property in properties)
            {
                if (!includeMetadata && property.Path.StartsWith("@metadata"))
                    continue;

                var valueIsObject = property.Value is RavenJObject;

                if (!valueIsObject || !excludeParentPropertyNames)
                    yield return property.Path;

                if (includeNestedProperties && valueIsObject)
                {
                    foreach (var childProperty in GetPropertiesFromJObject(property.Value as RavenJObject, property.Path, true))
                    {
                        yield return childProperty;
                    }
                }
            }
        }
    }
}
