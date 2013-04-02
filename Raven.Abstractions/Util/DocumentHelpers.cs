using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Abstractions.Util
{
    public static class DocumentHelpers
    {
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